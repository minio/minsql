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
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/minio/minio/pkg/certs"
)

func handleSignals(server *http.Server, tlsCerts *certs.Certs, httpServerErrorCh chan error, osSignalCh chan os.Signal) {
	// Custom exit function
	exit := func(state bool) {
		if state {
			os.Exit(0)
		}

		os.Exit(1)
	}

	stopProcess := func() bool {
		// Stop watching for any certificate changes.
		tlsCerts.Stop()

		// Create a deadline to wait for shutdown.
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// Doesn't block if no connections, but will otherwise wait
		// until the timeout deadline.
		return server.Shutdown(ctx) == nil
	}

	for {
		select {
		case err := <-httpServerErrorCh:
			exit(err == nil)
		case osSignal := <-osSignalCh:
			log.Printf("Exiting on signal: %s\n", strings.ToUpper(osSignal.String()))
			exit(stopProcess())
		}
	}
}
