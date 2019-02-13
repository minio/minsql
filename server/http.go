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
	"crypto/tls"
	"net/http"
	"os"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/pkg/certs"
)

const (
	// DefaultTCPKeepAliveTimeout - default TCP keep alive timeout for accepted connection.
	DefaultTCPKeepAliveTimeout = 30 * time.Second

	// DefaultMaxHeaderBytes - default maximum HTTP header size in bytes.
	DefaultMaxHeaderBytes = 1 * humanize.MiByte
)

// Secure Go implementations of modern TLS ciphers
// The following ciphers are excluded because:
//  - RC4 ciphers:              RC4 is broken
//  - 3DES ciphers:             Because of the 64 bit blocksize of DES (Sweet32)
//  - CBC-SHA256 ciphers:       No countermeasures against Lucky13 timing attack
//  - CBC-SHA ciphers:          Legacy ciphers (SHA-1) and non-constant time
//                              implementation of CBC.
//                              (CBC-SHA ciphers can be enabled again if required)
//  - RSA key exchange ciphers: Disabled because of dangerous PKCS1-v1.5 RSA
//                              padding scheme. See Bleichenbacher attacks.
var defaultCipherSuites = []uint16{
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
}

// Go only provides constant-time implementations of Curve25519 and NIST P-256 curve.
var secureCurves = []tls.CurveID{tls.X25519, tls.CurveP256}

func newHTTPServer(address string) (*http.Server, *certs.Certs, error) {
	// Check and load TLS certificates.
	tlsCerts, err := certs.New(getPublicCertFile(), getPrivateKeyFile(), loadX509KeyPair)
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}

	var tlsConfig *tls.Config
	if tlsCerts != nil {
		tlsConfig = &tls.Config{
			// TLS hardening
			PreferServerCipherSuites: true,
			CipherSuites:             defaultCipherSuites,
			CurvePreferences:         secureCurves,
			MinVersion:               tls.VersionTLS12,
			NextProtos:               []string{"h2", "http/1.1"},
			GetCertificate:           tlsCerts.GetCertificate,
		}
	}

	return &http.Server{
		Addr:           address,
		TLSConfig:      tlsConfig,
		MaxHeaderBytes: DefaultMaxHeaderBytes,
		IdleTimeout:    120 * time.Second,
	}, tlsCerts, nil
}
