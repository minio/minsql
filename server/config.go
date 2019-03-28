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
	"io"
	"time"

	"github.com/BurntSushi/toml"

	minio "github.com/minio/minio-go"
)

const (
	defaultConfigBucket = "config"
	defaultConfigFile   = "config.toml"

	configVersion = "1"
)

type minSQLConfig struct {
	Version    string                         `toml:"version"`
	Datastores map[string]dataStoreInfo       `toml:"datastore"`
	Tables     map[string]tableInfo           `toml:"table"`
	Auth       map[string]map[string]authInfo `toml:"auth"`
}

type authStatus string

const (
	authEnabled  authStatus = "enabled"
	authDisabled authStatus = "disabled"
)

type authInfo struct {
	Token  string        `json:"token" toml:"token"`
	API    []string      `json:"api" toml:"api"`
	Expire time.Duration `json:"expire" toml:"expire"`
	Status authStatus    `json:"status" toml:"status"`
}

type dataStoreInfo struct {
	Endpoint  string `json:"endpoint" toml:"endpoint"`
	AccessKey string `json:"access_key,omitempty" toml:"access_key"`
	SecretKey string `json:"secret_key,omitempty" toml:"secret_key"`
	Bucket    string `json:"bucket" toml:"bucket"`
	Prefix    string `json:"prefix" toml:"prefix"`
}

type tableInfo struct {
	Datastores            []string `json:"datastores" toml:"datastores"`
	OutputRecordDelimiter string   `json:"output_record_delimiter" toml:"output_record_delimiter"`
}

func initMinSQLConfig(client *minio.Client) (*minSQLConfig, error) {
	config := &minSQLConfig{
		Version:    configVersion,
		Datastores: make(map[string]dataStoreInfo),
		Tables:     make(map[string]tableInfo),
		Auth:       make(map[string]map[string]authInfo),
	}

	r, w := io.Pipe()

	te := toml.NewEncoder(w)

	go func() {
		w.CloseWithError(te.Encode(config))
	}()

	_, err := client.PutObject(defaultConfigBucket, defaultConfigFile, r, -1, minio.PutObjectOptions{})
	if err != nil {
		return nil, err
	}

	return config, r.Close()
}

func readMinSQLConfig(client *minio.Client) (*minSQLConfig, error) {
	configReader, err := client.GetObject(defaultConfigBucket, defaultConfigFile, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	config := &minSQLConfig{
		Datastores: make(map[string]dataStoreInfo),
		Tables:     make(map[string]tableInfo),
		Auth:       make(map[string]map[string]authInfo),
	}

	if _, err = toml.DecodeReader(configReader, config); err != nil {
		return nil, err
	}

	for table := range config.Tables {
		if !validTable.MatchString(table) {
			return nil, fmt.Errorf("%s table name invalid, should have alphanumeric characters such as [helloWorld0, hello_World0, ...]", table)
		}
	}
	return config, nil
}
