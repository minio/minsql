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
	"fmt"
	"net/http"
	"path"
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
