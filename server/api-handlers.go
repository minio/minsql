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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/gorilla/mux"
	minio "github.com/minio/minio-go"
)

type apiHandlers struct {
	client *minio.Client
}

func (a apiHandlers) IngestHandler(w http.ResponseWriter, r *http.Request) {
	// Add authentication here once we finalize on which authentication
	// style to use.

	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	prefixName := vars["prefix"] // optional

	epoch := fmt.Sprintf("%x", time.Now().Unix())
	for chunk := range Chunker(r.Body, 10*humanize.MiByte) {
		if chunk.Err != nil {
			http.Error(w, chunk.Err.Error(), http.StatusBadRequest)
			return
		}
		objectName := path.Join(prefixName, epoch, fmt.Sprintf("%d.chunk", chunk.Index))
		_, err := a.client.PutObject(bucketName, objectName, bytes.NewReader(chunk.Data), int64(len(chunk.Data)), minio.PutObjectOptions{})
		if err != nil {
			http.Error(w, chunk.Err.Error(), http.StatusBadRequest)
			return
		}
	}
}

// selectObject is to run query and writes the obtained output
func (a apiHandlers) selectObject(wg *sync.WaitGroup, w http.ResponseWriter, bucket string, opts minio.SelectObjectOptions, objectCh chan string) {
	select {
	case object := <-objectCh:
		defer wg.Done()
		sresults, _ := a.client.SelectObjectContent(context.Background(), bucket, object, opts)
		if sresults != nil {
			defer sresults.Close()
			io.Copy(w, sresults)
			w.(http.Flusher).Flush()
		}
	}
}

// QueryHandler - run a query on an blob or a collection of blobs.
//
// GET /api/{bucket}?prefix={prefix}&sql={sql} HTTP/2.0
// Host: minsql:9999
// Date: Mon, 3 Oct 2016 22:32:00 GMT
//
// HTTP/2.0 200 OK
// ...
// ...
//
// Examples:
// ## Unauthorized
// ~ curl http://minsql:9999/api/testbucket?prefix=jsons%2F&sql=select+s.key+from+s3object+s+where+s.size+%3E+1000
//
// ## Authorized
// ~ curl -H "Authorization: auth" http://minsql:9999/api/testbucket?prefix=jsons%2F&sql=select+s.key+from+s3object+s+where+s.size+%3E+1000
func (a apiHandlers) QueryHandler(w http.ResponseWriter, r *http.Request) {
	// Add authentication here once we finalize on which authentication
	// style to use.

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	prefix := vars["prefix"]
	sql := vars["sql"]

	if sql == "" {
		sql = "select * from s3object"
	}

	// Initialize the default select options.
	opts := minio.SelectObjectOptions{
		Expression:     sql,
		ExpressionType: minio.QueryExpressionTypeSQL,
		InputSerialization: minio.SelectObjectInputSerialization{
			JSON: &minio.JSONInputOptions{
				Type: minio.JSONLinesType,
			},
		},
		OutputSerialization: minio.SelectObjectOutputSerialization{
			JSON: &minio.JSONOutputOptions{
				RecordDelimiter: "\n",
			},
		},
	}

	var wg = &sync.WaitGroup{}
	ch := make(chan string, runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go a.selectObject(wg, w, bucket, opts, ch)
	}

	doneCh := make(chan struct{}, 1)
	defer close(doneCh)

	for obj := range a.client.ListObjects(bucket, prefix, true, doneCh) {
		if obj.Size > 0 && !strings.HasSuffix(obj.Key, "/") {
			ch <- obj.Key
		}
	}

	close(ch)
	wg.Wait()
}
