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
	"io"

	"github.com/BurntSushi/toml"

	minio "github.com/minio/minio-go"
)

const (
	defaultConfigBucket = "config"
	defaultConfigFile   = "config.toml"
)

type minSQLConfig struct {
	Servers map[string]serverInfo `toml:"servers"`
	Tables  map[string]tableInfo  `toml:"tables"`
}

type serverInfo struct {
	EndpointURL string `toml:"endpoint_url"`
	AccessKey   string `toml:"access_key"`
	SecretKey   string `toml:"secret_key"`
}

type tableInfo struct {
	Alias                 string `toml:"server_alias"`
	Bucket                string `toml:"bucket"`
	Prefix                string `toml:"prefix"`
	OutputRecordDelimiter string `toml:"output_record_delimiter"`
}

func initMinSQLConfig(client *minio.Client) (*minSQLConfig, error) {
	config := &minSQLConfig{
		Servers: make(map[string]serverInfo),
		Tables:  make(map[string]tableInfo),
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
		Servers: make(map[string]serverInfo),
		Tables:  make(map[string]tableInfo),
	}

	if _, err = toml.DecodeReader(configReader, config); err != nil {
		return nil, err
	}

	return config, nil
}
