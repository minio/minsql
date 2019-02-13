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
	"crypto"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// TLSPrivateKeyPassword is the environment variable which contains the password used
// to decrypt the TLS private key. It must be set if the TLS private key is
// password protected.
const TLSPrivateKeyPassword = "MINSQL_CERT_PASSWD"

func parsePublicCertFile(certFile string) (x509Certs []*x509.Certificate, err error) {
	// Read certificate file.
	var data []byte
	if data, err = ioutil.ReadFile(certFile); err != nil {
		return nil, err
	}

	// Trimming leading and tailing white spaces.
	data = bytes.TrimSpace(data)

	// Parse all certs in the chain.
	current := data
	for len(current) > 0 {
		var pemBlock *pem.Block
		if pemBlock, current = pem.Decode(current); pemBlock == nil {
			return nil, fmt.Errorf("Could not read PEM block from file %s", certFile)
		}

		var x509Cert *x509.Certificate
		if x509Cert, err = x509.ParseCertificate(pemBlock.Bytes); err != nil {
			return nil, err
		}

		x509Certs = append(x509Certs, x509Cert)
	}

	if len(x509Certs) == 0 {
		return nil, fmt.Errorf("Empty public certificate file %s", certFile)
	}

	return x509Certs, nil
}

func getRootCAs(certsCAsDir string) (*x509.CertPool, error) {
	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		// In some systems (like Windows) system cert pool is
		// not supported or no certificates are present on the
		// system - so we create a new cert pool.
		rootCAs = x509.NewCertPool()
	}

	fis, err := ioutil.ReadDir(certsCAsDir)
	if err != nil {
		if os.IsNotExist(err) {
			// Return success if CA's directory is missing.
			err = nil
		}
		return rootCAs, err
	}

	// Load all custom CA files.
	for _, fi := range fis {
		// Skip all directories.
		if fi.IsDir() {
			continue
		}
		caCert, err := ioutil.ReadFile(filepath.Join(certsCAsDir, fi.Name()))
		if err != nil {
			return rootCAs, err
		}
		rootCAs.AppendCertsFromPEM(caCert)
	}
	return rootCAs, nil
}

// load an X509 key pair (private key , certificate) from the provided
// paths. The private key may be encrypted and is decrypted using the
// ENV_VAR: MINIO_CERT_PASSWD.
func loadX509KeyPair(certFile, keyFile string) (tls.Certificate, error) {
	certPEMBlock, err := ioutil.ReadFile(certFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPEMBlock, err := ioutil.ReadFile(keyFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	key, rest := pem.Decode(keyPEMBlock)
	if len(rest) > 0 {
		return tls.Certificate{}, errors.New("The private key contains additional data")
	}
	if x509.IsEncryptedPEMBlock(key) {
		password, ok := os.LookupEnv(TLSPrivateKeyPassword)
		if !ok {
			return tls.Certificate{}, errors.New("No password set for TLS private key")
		}
		decryptedKey, decErr := x509.DecryptPEMBlock(key, []byte(password))
		if decErr != nil {
			return tls.Certificate{}, decErr
		}
		keyPEMBlock = pem.EncodeToMemory(&pem.Block{Type: key.Type, Bytes: decryptedKey})
	}
	cert, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
	if err != nil {
		return tls.Certificate{}, err
	}
	// Ensure that the private key is not a P-384 or P-521 EC key.
	// The Go TLS stack does not provide constant-time implementations of P-384 and P-521.
	if priv, ok := cert.PrivateKey.(crypto.Signer); ok {
		if pub, ok := priv.Public().(*ecdsa.PublicKey); ok {
			if name := pub.Params().Name; name == "P-384" || name == "P-521" { // unfortunately there is no cleaner way to check
				return tls.Certificate{}, fmt.Errorf("tls: the ECDSA curve '%s' is not supported", name)
			}
		}
	}
	return cert, nil
}

// isFile - returns whether given path is a file or not.
func isFile(path string) bool {
	if fi, err := os.Stat(path); err == nil {
		return fi.Mode().IsRegular()
	}

	return false
}
