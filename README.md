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
version = 1

[datastore]
    [datastore.play]
    endpoint = "https://play.minio.io:9000"
    access_key = "Q3AM3UQ867SPQQA43P2F"
    secret_key = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
    bucket = "testbucket1"
    prefix = ""

    [datastore.myminio]
    endpoint = "https://play.minio.io:9000"
    access_key = "Q3AM3UQ867SPQQA43P2F"
    secret_key = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
    bucket = "testbucket2"
    prefix = ""

[table]
    [table.temperature1]
    datastores = ["play", "myminio"]

[auth]
    [auth.NAME1]
        [auth.NAME1.temperature1]
        token = TOKEN1
        api = [search]
        expire = TIME
        status = enabled
    [auth.NAME2]
        [auth.NAME2.temperature2]
        token = TOKEN2
        api = [search, log]
        expire = TIME
        status = disabled
```

Upload the new config
```
mc cp config.toml play/config/config.toml
```

> NOTE: There is no need to restart the MinSQL server, config will be reloaded automatically.

## Search API
```
curl http://localhost:9999/search/ --data "select * from json"
```

## Log API
```
curl http://localhost:9999/log/ --data @/tmp/log.json
```
