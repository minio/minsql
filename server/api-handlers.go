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
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"strings"
	"sync"

	"github.com/gorilla/mux"
	minio "github.com/minio/minio-go"
	xnet "github.com/minio/minio/pkg/net"
)

type apiHandlers struct {
	sync.RWMutex
	configClnt *minio.Client
	config     *minSQLConfig
}

func (a *apiHandlers) IngestHandler(w http.ResponseWriter, r *http.Request) {
	// Add authentication here once we finalize on which authentication
	// style to use.

	vars := mux.Vars(r)
	_ = vars
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

// QueryHandler - run a query on an blob or a collection of blobs.
//
// GET /api/sql={sql} HTTP/2.0
// Host: minsql:9999
// Date: Mon, 3 Oct 2016 22:32:00 GMT
//
// HTTP/2.0 200 OK
// ...
//
// Examples:
// ## Use POST form to query the table
// ~ curl http://minsql:9999/api -F 'sql=select s.key from tablename s where s.size > 1000'
//
// ## With Authorization
// ~ curl -H "Authorization: auth"  http://minsql:9999/api -F 'sql=select s.key from tablename s where s.size > 1000'
func (a *apiHandlers) QueryHandler(w http.ResponseWriter, r *http.Request) {
	// Add authentication here once we finalize on which authentication
	// style to use.

	sql := r.PostFormValue("sql")

	table, err := GetTableName(sql)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	a.RLock()
	tblInfo, ok := a.config.Tables[table]
	a.RUnlock()
	if !ok {
		http.Error(w, fmt.Sprintf("%s table not found", table), http.StatusNotFound)
		return
	}

	a.RLock()
	sinfo, ok := a.config.Servers[tblInfo.Alias]
	a.RUnlock()
	if !ok {
		http.Error(w, fmt.Sprintf("server alias %s not found for the table %s", tblInfo.Alias, table), http.StatusNotFound)
		return
	}

	// Initialize the default select options.
	opts := minio.SelectObjectOptions{
		Expression:     strings.Replace(sql, fmt.Sprintf("from %s", table), "from s3object", -1),
		ExpressionType: minio.QueryExpressionTypeSQL,
		InputSerialization: minio.SelectObjectInputSerialization{
			JSON: &minio.JSONInputOptions{
				Type: minio.JSONLinesType,
			},
		},
		OutputSerialization: minio.SelectObjectOutputSerialization{
			JSON: &minio.JSONOutputOptions{
				RecordDelimiter: tblInfo.OutputRecordDelimiter,
			},
		},
	}

	endpointURL, err := xnet.ParseURL(sinfo.EndpointURL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	sclient, err := minio.NewV4(endpointURL.Host, sinfo.AccessKey, sinfo.SecretKey, endpointURL.Scheme == "https")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var wg = &sync.WaitGroup{}
	ch := make(chan string, runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			object, ok := <-ch
			if ok {
				sresults, _ := sclient.SelectObjectContent(context.Background(), tblInfo.Bucket, object, opts)
				if sresults != nil {
					defer sresults.Close()
					io.Copy(w, sresults)
					w.(http.Flusher).Flush()
				}
			}
		}()
	}

	doneCh := make(chan struct{}, 1)
	defer close(doneCh)

	for obj := range sclient.ListObjects(tblInfo.Bucket, tblInfo.Prefix, true, doneCh) {
		if obj.Size > 0 && !strings.HasSuffix(obj.Key, "/") {
			ch <- obj.Key
		}
	}

	close(ch)
	wg.Wait()
}
