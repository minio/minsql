/*
 * MinSQL, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/bcicen/jstream"
	"github.com/gorilla/mux"
	minio "github.com/minio/minio-go"
	xnet "github.com/minio/minio/pkg/net"

	"github.com/skyrings/skyring-common/tools/uuid"
	pfile "github.com/xitongsys/parquet-go/ParquetFile"
	pwriter "github.com/xitongsys/parquet-go/ParquetWriter"
)

func mustGetUUID() string {
	uuid, err := uuid.New()
	if err != nil {
		panic(err)
	}

	return uuid.String()
}

type apiHandlers struct {
	sync.RWMutex
	configClnt *minio.Client
	config     *minSQLConfig
}

// Reader - JSON record reader for S3Select.
type Reader struct {
	decoder    *jstream.Decoder
	valueCh    chan *jstream.MetaValue
	readCloser io.ReadCloser
}

// Read - reads single record.
func (r *Reader) Read() (jstream.KVS, error) {
	v, ok := <-r.valueCh
	if !ok {
		if err := r.decoder.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	if v.ValueType != jstream.Object {
		return nil, errors.New("unexpected json object")
	}

	// This is a JSON object type (that preserves key
	// order)
	return v.Value.(jstream.KVS), nil
}

// Close - closes underlaying reader.
func (r *Reader) Close() error {
	return r.readCloser.Close()
}

func toParquetType(value interface{}) string {
	switch value.(type) {
	case string:
		return "UTF8, encoding=PLAIN_DICTIONARY"
	case float32:
		return "FLOAT"
	case float64:
		return "DOUBLE"
	case int32:
		return "INT32"
	case int64:
		return "INT64"
	case bool:
		return "BOOLEAN"
	case []byte:
		return "BYTE_ARRAY"
	case []interface{}:
		return "LIST"
	case map[interface{}]interface{}:
		return "MAP"
	}
	return "UNKNOWN"
}

func inferSchema(kvs jstream.KVS, table string) ([]byte, error) {
	schemaKVS := jstream.KVS{}
	schemaKVS = append(schemaKVS, jstream.KV{
		Key:   "Tag",
		Value: "name=" + table,
	})

	fieldsKV := jstream.KV{
		Key: "Fields",
	}

	var fields []jstream.KVS
	for _, kv := range kvs {
		vtype := toParquetType(kv.Value)
		if vtype != "UNKNOWN" {
			fields = append(fields, jstream.KVS{
				jstream.KV{
					Key: "Tag",
					Value: fmt.Sprintf("name=%s, type=%s",
						kv.Key, vtype),
				},
			})
		}
	}
	fieldsKV.Value = fields
	schemaKVS = append(schemaKVS, fieldsKV)
	return json.Marshal(schemaKVS)
}

func (a *apiHandlers) tblInfoToDataStores(tinfo tableInfo, table string) ([]dataStore, error) {
	var dsts []dataStore
	for _, datastore := range tinfo.Datastores {
		a.RLock()
		sinfo, ok := a.config.Datastores[datastore]
		if !ok {
			return nil, fmt.Errorf("datastore %s not found for the table %s", datastore, table)
		}
		a.RUnlock()
		endpoint, err := xnet.ParseURL(sinfo.Endpoint)
		if err != nil {
			return nil, err
		}

		sclient, err := minio.NewV4(endpoint.Host, sinfo.AccessKey, sinfo.SecretKey, endpoint.Scheme == "https")
		if err != nil {
			return nil, err
		}

		dsts = append(dsts, dataStore{
			client: sclient,
			bucket: sinfo.Bucket,
			prefix: sinfo.Prefix,
		})
	}
	return dsts, nil
}

var (
	validTable = regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9-_]+$")
)

func shuffle(dsts []dataStore) []dataStore {
	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// We start at the end of the slice, inserting our random
	// values one at a time.
	for n := len(dsts); n > 0; n-- {
		randIndex := rand.Intn(n)
		// We swap the value at index n-1 and the random index
		// to move our randomly chosen value to the end of the
		// slice, and to move the value that was at n-1 into our
		// unshuffled portion of the slice.
		dsts[n-1], dsts[randIndex] = dsts[randIndex], dsts[n-1]
	}

	return dsts
}

// ListTablesHandler - list all configured tables
//
// GET /list HTTP/2.0
// Host: minsql:9999
// Date: Mon, 3 Oct 2016 22:32:00 GMT
//
//
//
// HTTP/2.0 200 OK
// ...
// ...
// ["temperature"]
//
// Examples:
// ## Use GET to list all tables
// ~ curl http://minsql:9999/list
//
// ## With Authorization
// ~ curl -H "Authorization: auth" http://minsql:9999/list
func (a *apiHandlers) ListTablesHandler(w http.ResponseWriter, r *http.Request) {
	var tables []string
	a.RLock()
	for k := range a.config.Tables {
		tables = append(tables, k)
	}
	a.RUnlock()

	encoder := json.NewEncoder(w)
	encoder.Encode(tables)
	w.(http.Flusher).Flush()
}

const timeFormat = "2006/Jan/02/15-04-05"

// LogIngestHandler - run a query on an blob or a collection of blobs.
//
// POST /log/{tablename} HTTP/2.0
// Host: minsql:9999
// Date: Mon, 3 Oct 2016 22:32:00 GMT
//
// {"status":"success","type":"folder","lastModified":"2019-03-11T17:58:55.197224468-07:00","size":220,"key":"objectname","etag":""}
//
//
// HTTP/2.0 200 OK
// ...
//
// Examples:
// ## Use POST form to search the table
// ~ curl http://minsql:9999/log/{tablename} --data @log.json
//
// ## With Authorization
// ~ curl -H "Authorization: auth" http://minsql:9999/log/{tablename} --data @log.json
func (a *apiHandlers) LogIngestHandler(w http.ResponseWriter, r *http.Request) {
	// Add authentication here once we finalize on which authentication
	// style to use.
	vars := mux.Vars(r)
	table := vars["table"]

	if !validTable.MatchString(table) {
		http.Error(w, fmt.Sprintf("%s table name is invalid", table), http.StatusBadRequest)
		return
	}

	a.RLock()
	tblInfo, ok := a.config.Tables[table]
	a.RUnlock()
	if !ok {
		http.Error(w, fmt.Sprintf("%s table not found", table), http.StatusNotFound)
		return
	}

	d := jstream.NewDecoder(r.Body, 0).ObjectAsKVS()
	jr := &Reader{
		decoder:    d,
		valueCh:    d.Stream(),
		readCloser: r.Body,
	}

	kvs, err := jr.Read()
	if err != nil && err != io.EOF {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// we reached EOF before schema inference, no data sent by client.
	if err == io.EOF {
		return
	}

	schemaBytes, err := inferSchema(kvs, table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	dsts, err := a.tblInfoToDataStores(tblInfo, table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	uuid := mustGetUUID()
	parquetTable := table + ".parquet"

	var done bool
	for !done {
		if done {
			return
		}
		fw, err := pfile.NewLocalFileWriter("stg.parquet")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer os.Remove("stg.parquet")
		pw, err := pwriter.NewJSONWriter(string(schemaBytes), fw, 4)
		if err != nil {
			fw.Close()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		count := 100000 // Write 100k records per parquet file.
		for count > 0 {
			var kvBytes []byte
			kvBytes, err = json.Marshal(kvs)
			if err != nil {
				pw.WriteStop()
				fw.Close()
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if err = pw.Write(string(kvBytes)); err != nil {
				pw.WriteStop()
				fw.Close()
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			kvs, err = jr.Read()
			if err != nil && err != io.EOF {
				pw.WriteStop()
				fw.Close()
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if err == io.EOF {
				done = true
				break
			}

			count--
		}

		pw.WriteStop()
		fw.Close()

		dst := shuffle(dsts)[0]
		name := path.Join(dst.prefix, parquetTable,
			time.Now().UTC().Format(timeFormat),
			fmt.Sprintf("%s.snappy.parquet", uuid))
		if _, err = dst.client.FPutObject(dst.bucket, name, "stg.parquet", minio.PutObjectOptions{}); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
}

func (a *apiHandlers) watchMinSQLConfig() {
	doneCh := make(chan struct{})
	defer close(doneCh)

	var events []string
	events = append(events, string(minio.ObjectCreatedAll))
	events = append(events, string(minio.ObjectRemovedAll))

	nch := a.configClnt.ListenBucketNotification(defaultConfigBucket, defaultConfigFile, "", events, doneCh)
	for n := range nch {
		if n.Err != nil {
			log.Println(n.Err)
			return
		}
		var err error
		for _, nrecord := range n.Records {
			a.Lock()
			if strings.HasPrefix(nrecord.EventName, "s3:ObjectCreated:") {
				a.config, err = readMinSQLConfig(a.configClnt)
			} else if strings.HasPrefix(nrecord.EventName, "s3:ObjectRemoved:") {
				a.config, err = initMinSQLConfig(a.configClnt)
			}
			a.Unlock()
			if err != nil {
				log.Println(err)
				return
			}
		}
	}
}

type dataStore struct {
	client *minio.Client
	bucket string
	prefix string
}

// SearchHandler - run a query on an blob or a collection of blobs.
//
// POST /search HTTP/2.0
// Host: minsql:9999
// Date: Mon, 3 Oct 2016 22:32:00 GMT
// Content-Type: application/x-www-form-urlencoded
//
// select s.key from json s where s.size > 1000
//
// HTTP/2.0 200 OK
// ...
//
// Examples:
// ## Use POST form to search the table
// ~ curl http://minsql:9999/search --data 'select s.key from tablename s where s.size > 1000'
//
// ## With Authorization
// ~ curl -H "Authorization: auth" http://minsql:9999/search --data 'select s.key from tablename s where s.size > 1000'
func (a *apiHandlers) SearchHandler(w http.ResponseWriter, r *http.Request) {
	// Add authentication here once we finalize on which authentication
	// style to use.

	const maxFormSize = int64(10 << 20) // 10 MB is a lot of text.
	sqlBytes, err := ioutil.ReadAll(io.LimitReader(r.Body, maxFormSize+1))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if int64(len(sqlBytes)) > maxFormSize {
		http.Error(w, "http: POST too large", http.StatusBadRequest)
		return
	}

	sql := string(sqlBytes)

	table, err := GetTableName(sql)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !validTable.MatchString(table) {
		http.Error(w, fmt.Sprintf("%s table name invalid", table), http.StatusBadRequest)
		return
	}

	a.RLock()
	tblInfo, ok := a.config.Tables[table]
	a.RUnlock()
	if !ok {
		http.Error(w, fmt.Sprintf("%s table not found", table), http.StatusNotFound)
		return
	}

	// Initialize the default select options.
	opts := minio.SelectObjectOptions{
		Expression:     strings.Replace(sql, fmt.Sprintf("from %s", table), "from s3object", -1),
		ExpressionType: minio.QueryExpressionTypeSQL,
		InputSerialization: minio.SelectObjectInputSerialization{
			Parquet: &minio.ParquetInputOptions{},
		},
		OutputSerialization: minio.SelectObjectOutputSerialization{
			JSON: &minio.JSONOutputOptions{
				RecordDelimiter: "\n",
			},
		},
	}

	dsts, err := a.tblInfoToDataStores(tblInfo, table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var wg = &sync.WaitGroup{}
	ch := make(chan dataStore, runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ds, ok := <-ch
			if ok {
				sresults, _ := ds.client.SelectObjectContent(context.Background(), ds.bucket, ds.prefix, opts)
				if sresults != nil {
					io.Copy(w, sresults)
					w.(http.Flusher).Flush()
					sresults.Close()
				}
			}
		}()
	}

	doneCh := make(chan struct{}, 1)
	defer close(doneCh)

	for _, dst := range dsts {
		for obj := range dst.client.ListObjects(dst.bucket, path.Join(dst.prefix, table), true, doneCh) {
			if obj.Size > 0 && !strings.HasSuffix(obj.Key, "/") {
				ch <- dataStore{
					client: dst.client,
					bucket: dst.bucket,
					prefix: obj.Key,
				}
			}
		}
	}

	close(ch)
	wg.Wait()
}
