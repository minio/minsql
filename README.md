# MinSQL

Massively Parallel Processing (MPP) log database with a simple HTTP API. For usage instructions, click [here](#Usage).

MinSQL stores data in Columnar-Parquet format ordered by time. It is built on top of object storage for persistence. Logs can grow indefinitely across multiple MinIO clusters.

## What is a log database?

Log database is a type of database that is optimized for ingesting JSON data records (log lines, events, messages etc.) at massive scales with SQL-like search/query capabilities.

## Features

- [x] Ingestion
- [x] Search by applying operators on key/value
- [x] Shared Nothing Architecture
- [x] Parquet data storage format
- [x] Petabyte scale
- [x] Backed by MinIO
- [x] S3 Select compatibility
- [x] Search across MinIO clusters and buckets
- [x] Simple HTTP Interface
- [x] JSON schema evolution
- [ ] Approximate pattern matching

## What MinSQL is **NOT**

MinSQL does **NOT** try to be

- A Relational database
- A Search engine for non-log data
- A Message Queue
- A Transactional database with write guarantees
- A Database with `Join`, `GroupBy` or `ACID` guarantees

## Architecture

![Architecture](./architecture.png)

# Usage

## Install
```sh
$ go get -d github.com/minio/minsql
$ cd $GOPATH/src/github.com/minio/minsql
$ make dockerbuild
$ ./minsql --help
Distributed SQL based search engine for log data

MinSQL DEVELOPMENT.GOGET by MinIO Inc.

Usage:
  minsql

Environment:
  MINIO_ENDPOINT    SCHEME://ADDRESS:PORT of the minio endpoint
  MINIO_ACCESS_KEY  Access key for the minio endpoint
  MINIO_SECRET_KEY  Secret key for the minio endpoint

Flags:
      --address string     bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname (default ":9999")
      --certs-dir string   path to certs directory
  -h, --help               help for minsql
      --version            version for minsql
```

## Setup

The setup requires two steps

1. Configure MinSQL to connect to the MinIO backend
2. Configure MinIO backend(s) with table to bucket mappings

### Step 1: Configuring MinSQL

Export the following environment variable to configure

- MinIO endpoint - the address at which MinIO server is running
- MinIO access key - the access key for the MinIO server
- MinIO secret key - the secret key corresponding to the access key

```sh
$ export MINIO_ENDPOINT=https://play.minio.io:9000
$ export MINIO_ACCESS_KEY=Q3AM3UQ867SPQQA43P2F
$ export MINIO_SECRET_KEY=zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG
```

Note that the version of the MinIO backend needs to be `RELEASE.2019-01-10T00-21-20Z` or later

```sh
$ ./minsql
2019/03/04 10:02:49 MinSQL now listening on :9999
```

### Step 2: Configuring MinIO

MinSQL can search and ingest logs across MinIO clusters and buckets. Each one of these `datastores` map
tables to buckets. The configuration needs to be provided to the MinIO backend that was configured via environment
variables in Step 1.

The configuration format (in TOML) is laid out below with an example

```toml
version = 1

[table]
    [table.temperature1]
    datastores = ["play", "myminio"]

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

[auth]
    [auth.NAME1]
        [auth.NAME1.temperature1]
        token = "TOKEN1"
        api = ["search"]
        expire = "duration"
        status = "enabled"
    [auth.NAME2]
        [auth.NAME2.temperature2]
        token = "TOKEN2"
        api = ["search", "log"]
        expire = "duration"
        status = "disabled"
```

#### Tables

Tables are namespaces for grouping JSON documents with similar schema. The table section in the config defines the tables against which queries and ingestions will be done. Each table can have multiple datastores.

#### Datastores

Each Datastore is a pointer to a bucket. The `prefix` key defines the "folder" into which the ingested data will be stored and queried.

#### Authorization

Access and Secret key provide complete access to the data in MinIO. If you want to restrict the access to resources, then authorization tokens can be provisioned
and assigned to particular resources. The resources can be any combination of

- log
- search

Authorization tokens can also be provisioned with an expiry date to give time bound access to users. It is possible to revoke access tokens at any point.

In order to create an access token, simply set the token field to the password value of the token. Then choose the API resources for that token. If you would
like to set an expiry on the token, the amount of time to expiry should be specified in this format


```sh
14h0m32s
```

which would set an expiry of 14 hours and 32 seconds from the time when the token is provisioned. If your expiry is less than an hour away, then you do not need to
specify the hour section. For example

```sh
30m0s
```

would set the expiry to 30 minutes away. This format is based on the `time.Duration` serialization of golang standard time library.

Set the status field of the token to `enabled` or `disabled` based on your needs.

Once you've set these fields, Upload the new config to the backend using `mc`

```sh
$ export MINIO_ENDPOINT=https://play.minio.io:9000
$ export MINIO_ACCESS_KEY=Q3AM3UQ867SPQQA43P2F
$ export MINIO_SECRET_KEY=zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG
# create a bucket named 'config' (if it doesnt already exist)
$ mc mb play/config
# upload the config to the location config.toml in the config bucket
$ mc cp config.toml play/config/config.toml
```

### Dynamically updating the configuration

The config can be updated in MinIO backend at anypoint while MinSQL is in operation. It will automatically reload the config and work with the new parameters.

This can be used for adding or disabling authorization tokens, adding new tables or changing table information.

Now you are all set to start using MinSQL to ingest and search log data.

## Ingesting data

The ingestion API is also called the `log` API. We will use these two terms interchangeably. The log api is a HTTP POST endpoint with the following signature

```sh
POST /log/{table}
---
# BODY - Stream of JSON documents
{"key": "value", "data": "data"}
["value1", "value2"]
...
```

The `{table}` path parameter should have a table from the above defined configuration. If it doesnt, then it fails silently. This is an intentional design decision, since we allow dynamic updates of tables in real time, and at massive scales, small loss of data can be toleratored, but small failures can lead to a an unrecoverable backlog situation.

The body of the POST request should be a stream of JSON documents, any other data format will be ignored.

```sh
curl http://minsql:9999/log/tablename --data @log.json
```

## Search API

The search API is a HTTP POST endpoint with the following signature

```sh
POST /search
---
# Body - SQL like Select syntax
select s.data from tablename s where s.key=value
```

The syntax for querying is described [here](https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html)

There is one difference between standard S3 syntax and MinSQL syntax. In S3 syntax, the tablename is s3Object. In MinSQL, we use the tablename configured in the configuration.

```sh
curl http://minsql:9999/search --data 'select s.key from tablename s where s.size > 1000'
```
