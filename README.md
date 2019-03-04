# MinSQL
Distributed SQL based search engine for log data

> This project is currently work in progress, more details to come soon.

## Install
```sh
go get github.com/minio/minsql
minsql -h
```

## Run
```
export MINIO_ENDPOINT=https://play.minio.io:9000
export MINIO_ACCESS_KEY=Q3AM3UQ867SPQQA43P2F
export MINIO_SECRET_KEY=zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG
minsql
2019/03/04 10:02:49 MinSQL now listening on :9999
```

## Create a datastore
Following example save it as `config.toml`
```toml
[servers]
  [servers.play]
  endpoint_url = "https://play.minio.io:9000"
  access_key = "Q3AM3UQ867SPQQA43P2F"
  secret_key = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
[tables]
  [tables.json]
  server_alias = "play"
  bucket = "testbucket"
  prefix = "jsons/"
  output_record_delimiter = "\n"
```

Upload the new config
```
mc cp config.toml play/config/config.toml
```

> NOTE: There is no need to restart the MinSQL server, config will be reloaded automatically.

## Send query
```
curl http://localhost:9999/api/?sql=select+*+from+s3object&table=json
```
