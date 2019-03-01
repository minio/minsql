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
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"os"
	"time"

	"golang.org/x/net/http2"

	"github.com/minio/cli"
	minio "github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/credentials"
	xnet "github.com/minio/minio/pkg/net"
)

func newCustomDialContext(timeout time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := &net.Dialer{
			Timeout:   timeout,
			KeepAlive: timeout,
			DualStack: true,
		}

		return dialer.DialContext(ctx, network, addr)
	}
}

func newMinioAPI(ctx *cli.Context) (*minio.Client, error) {
	endpoint, ok := os.LookupEnv("MINIO_ENDPOINT")
	if !ok {
		return nil, errors.New("minio endpoint missing")
	}
	accessKey, ok := os.LookupEnv("MINIO_ACCESS_KEY")
	if !ok {
		return nil, errors.New("minio access key missing")
	}
	secretKey, ok := os.LookupEnv("MINIO_SECRET_KEY")
	if !ok {
		return nil, errors.New("minio secret key missing")
	}

	u, err := xnet.ParseURL(endpoint)
	if err != nil {
		return nil, err
	}

	creds := credentials.NewStaticV4(accessKey, secretKey, "")

	// By default enable HTTPs.
	useTLS := true
	if u.Scheme == "http" {
		useTLS = false
	}

	options := minio.Options{
		Creds:  creds,
		Secure: useTLS,
		Region: "",
	}

	client, err := minio.NewWithOptions(u.Host, &options)
	if err != nil {
		return nil, err
	}

	rootCAs, err := getRootCAs(defaultCertsCADir.Get())
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           newCustomDialContext(5 * time.Minute),
		MaxIdleConns:          4096,
		MaxIdleConnsPerHost:   4096,
		IdleConnTimeout:       120 * time.Second,
		TLSHandshakeTimeout:   30 * time.Second,
		ExpectContinueTimeout: 10 * time.Second,
		DisableCompression:    true,
	}

	if useTLS {
		// Keep TLS config.
		tlsConfig := &tls.Config{
			RootCAs: rootCAs,
			// Can't use SSLv3 because of POODLE and BEAST
			// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
			// Can't use TLSv1.1 because of RC4 cipher usage
			MinVersion: tls.VersionTLS12,
		}
		transport.TLSClientConfig = tlsConfig

		// Because we create a custom TLSClientConfig, we have to opt-in to HTTP/2.
		// See https://github.com/golang/go/issues/14275
		if err = http2.ConfigureTransport(transport); err != nil {
			return nil, err
		}
	}

	client.SetCustomTransport(transport)
	client.SetAppInfo("MinSQL", Version)

	return client, nil
}
