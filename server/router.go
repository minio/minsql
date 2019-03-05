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
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rakyll/statik/fs"

	"github.com/minio/cli"
	"github.com/minio/minio-go"

	// This is needed for webUI assets
	_ "github.com/minio/minsql/webui/assets"
)

const (
	apiRoutePrefix = "/api"
)

// statikFS returns the handler for the Web UI serving static
// file-server
func statikFS() http.Handler {
	statikFS, err := fs.New()
	if err != nil {
		log.Fatal(err)
	}
	return http.FileServer(statikFS)
}

func registerWebUIRouter(router *mux.Router) {
	router.Methods("GET").Path("/{index:.*}").Handler(statikFS())
}

func configureMinSQLHandler(ctx *cli.Context) (http.Handler, error) {
	client, err := newMinioAPI(ctx)
	if err != nil {
		return nil, err
	}

	config, err := readMinSQLConfig(client)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			config, err = initMinSQLConfig(client)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	// Initialize router. `SkipClean(true)` stops gorilla/mux from
	// normalizing URL path minio/minio#3256
	router := mux.NewRouter().SkipClean(true)

	// Initialize MinSQL API.
	api := &apiHandlers{
		configClnt: client,
		config:     config,
	}

	go api.watchMinSQLConfig()

	// API Router
	apiRouter := router.PathPrefix(apiRoutePrefix).Subrouter()

	bucketRouter := apiRouter.PathPrefix("/").Subrouter()

	// POST ingest API
	bucketRouter.Methods("POST").
		HeadersRegexp("Content-Type", "application/json*").
		HandlerFunc(api.IngestHandler)

	// GET query API
	bucketRouter.Methods("GET").
		Queries("sql", "{sql:.*}").
		HandlerFunc(api.QueryHandler)

	// Register web UI router.
	registerWebUIRouter(router)

	// Add future routes here.

	// If none of the routes match.
	apiRouter.NotFoundHandler = http.HandlerFunc(notFoundHandler)

	return router, nil
}

func notFoundHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, fmt.Sprintf("Request %s path not recognized", r.URL), http.StatusMethodNotAllowed)
}
