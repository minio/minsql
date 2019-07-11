> NOTE: This project is under development, please do not depend on it yet as things may break.

# MinSQL
MinSQL is a log search engine designed with simplicity in mind to the extent that no SDK is needed to interact with it, most programming languages and tools have some form of http request capability (ie: curl) and that's all you need to interact with MinSQL.

## To build
```
docker build . -t minio/minsql
docker run --rm minio/minsql --help
```

OR

```
make
./minsql --help
```

## Running the project
An instance of [MinIO](https://github.com/minio/minio) is needed as the storage engine for MinSQL. To keep things easier we have a `docker-compose` example for MinIO and MinSQL.

To run the project you need to provide the access details for a `Meta Bucket` to store the shared configuration between multiple `MinSQL` instances, the location and access to it should be configured via environment variables when starting MinSQL .

##### Binary:
```
export MINSQL_METABUCKET_NAME=minsql-meta
export MINSQL_METABUCKET_ENDPOINT=http://localhost:9000
export MINSQL_METABUCKET_ACCESS_KEY=minio
export MINSQL_METABUCKET_SECRET_KEY=minio123
./minsql
```

##### Docker
Create the compose file
```
cat > docker-compose.yml <<EOF
version: '2'

services:
 minio-engine:
  image: minio/minio
  volumes:
   - data:/data
  environment:
   MINIO_ACCESS_KEY: minio
   MINIO_SECRET_KEY: minio123
  command: server /data
 mc:
  image: minio/mc
  depends_on:
   - minio
  entrypoint: >
    /bin/sh -c "
    echo /usr/bin/mc config host a http://minio-engine:9000 minio minio123;
    /usr/bin/mc mb a/minsql-meta;
    "
 minsql:
  image: minio/minsql
  depends_on:
   - minio
   - mc
  ports:
   - "9999:9999"
  environment:
   MINSQL_METABUCKET_NAME: minsql-meta
   MINSQL_METABUCKET_ENDPOINT: http://minio-engine:9000
   MINSQL_ACCESS_KEY: minio
   MINSQL_SECRET_KEY: minio123

volumes:
  data:
EOF
```

```
docker-compose up
```

### Environment variables

| Environment                  | Description                                       |
| -------------                | -------------                                     |
| MINSQL_METABUCKET_NAME       | Name of the meta bucket                           |
| MINSQL_METABUCKET_ENDPOINT   | Name of the endpoint, ex: `http://localhost:9000` |
| MINSQL_METABUCKET_ACCESS_KEY | Meta Bucket Access key                            |
| MINSQL_METABUCKET_SECRET_KEY | Meta Bucket Secret key                            |
| MINSQL_PKCS12_CERT           | *Optional:* location to a pkcs12 certificate.     |
| MINSQL_PKCS12_PASSWORD       | *Optional:* password to unlock the certificate.   |

### Configuring

To start storing logs you need to setup a `DataStore`, `Log`, `Token` and a `Authorization` on MinSQL, this can be done using the admin REST APIs.

To get our sample code going we are going to:

1. `minioplay` datastore
1. `mylog` log
1. a token
1. authorize the token to our log

#### Add a sample datastore
Our sample datastore will be pointing to `play`, a demo instance of MinIO.

```bash
curl -X POST \
  http://127.0.0.1:9999/api/datastores \
  -H 'Content-Type: application/json' \
  -d '{
  "bucket" : "play-minsql",
  "endpoint" : "https://play.minio.io:9000",
  "prefix" : "",
  "name" : "minioplay",
  "access_key" : "Q3AM3UQ867SPQQA43P2F",
  "secret_key" : "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
}'
```

#### Add a Sample log
We are going to add a log `mylog` that stores it's contents on the `minioplay` datastore. 
```bash
curl -X POST \
  http://127.0.0.1:9999/api/logs \
  -H 'Content-Type: application/json' \
  -d '{
  "name" : "mylog",
  "datastores" : [
    "minioplay",
  ],
  "commit_window" : "5s"
}'
```

#### Create a sample token

We are going to generate a token with a hardcoded token `abcdefghijklmnopabcdefghijklmnopabcdefghijklmnop`

```bash
curl -X POST \
  http://127.0.0.1:9999/api/tokens \
  -H 'Content-Type: application/json' \
  -d '{
  "access_key" : "abcdefghijklmnop",
  "secret_key" : "abcdefghijklmnopabcdefghijklmnop",
  "description" : "test",
  "is_admin" : true,
  "enabled" : false
}'
```

#### Authorize token to log

Finally, we are going to authorize our new token to access `mylog`

```bash
curl -X POST \
  http://127.0.0.1:9999/api/auth/abcdefghijklmnop \
  -H 'Content-Type: application/json' \
  -d '{
  "log_name" : "mylog",
  "api" : ["search","store"]
}'
```

## Storing logs
For a log `mylog` defined on the configuration we can store logs on MinSQL by performing a `PUT` to your MinSQL instance

```
curl -X PUT \
  http://127.0.0.1:9999/mylog/store \
  -H 'MINSQL-TOKEN: TOKEN1' \
  -d '10.8.0.1 - - [16/May/2019:23:02:56 +0000] "GET / HTTP/1.1" 400 256 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:52.0) Gecko/20100101 Firefox/52.0"'
```

You can send multiple log lines separated by `new line`

## Querying logs
To get data out of MinSQL you can use SQL. Note that MinSQL is a data layer and not a computation layer, therefore certain SQL statements that need computations (SUM, MAX, GROUP BY, JOIN, etc...) are not supported.

All the query statements must be sent via `POST` to your MinSQL instance.

### SELECT
To select all the logs for a particular log you can perform a simple SELECT statement
```sql
SELECT * FROM mylog
```

And send that to MinSQL via POST
```
curl -X POST \
  http://127.0.0.1:9999/search \
  -H 'MINSQL-TOKEN: TOKEN1' \
  -d 'SELECT * FROM mylog'
```

This will return you all the raw log lines stored for that log.
```
67.164.164.165 - - [24/Jul/2017:00:16:46 +0000] "GET /info.php HTTP/1.1" 200 24564 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
67.164.164.165 - - [24/Jul/2017:00:16:48 +0000] "GET /favicon.ico HTTP/1.1" 404 209 "http://104.236.9.232/info.php" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
24.26.204.22 - - [24/Jul/2017:00:17:16 +0000] "GET /info.php HTTP/1.1" 200 24579 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
45.23.126.92 - - [24/Jul/2017:00:16:18 +0000] "GET /info.php HTTP/1.1" 200 24589 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
```

### Select parts of the data
We can get only parts of the data by using any of the supported MinSQL entities, which start with a `$` sign.

#### Positional
We can select from the data by its position, for example to get the first column and the fourth we can use `$1` and `$4`
```sql
SELECT $1, $4 FROM mylog;
```

To which MinSQL will reply
```
67.164.164.165 [24/Jul/2017:00:16:46
67.164.164.165 [24/Jul/2017:00:16:48
24.26.204.22 [24/Jul/2017:00:17:16
45.23.126.92 [24/Jul/2017:00:16:18
```

You can see that the data was selected as is, however the selected date column is not clean enough, MinSQL provides other entities to deal with this.

#### By Type
MinSQL provides a nice list of entities that make the extraction of data chunks from your raw data easy thanks to our powerful Schema on Read approach. For example we can select any ip in our data by using the entity `$ip` and any date using `$date`.
```sql
SELECT $ip, $date FROM mylog
```

To which MinSQL will reply
```
67.164.164.165 24/Jul/2017
67.164.164.165 24/Jul/2017
24.26.204.22 24/Jul/2017
45.23.126.92 24/Jul/2017
```

If your data contains more than one ip address you can access the subsequent ip's using positional entities.
```sql
SELECT $ip, $ip2, $ip3, $date FROM mylog
```

Please note that if no positional number is specified on an entity, it will default to the first position, in this case `$ip == $ip1`

## Filtering
Using the powerful select engine of MinSQL you can also filter the data so only the relevant information that you need to extract from your logs is returned.

For example, to filter out a single ip from your logs you could select by `$ip`

```sql
SELECT * FROM mylog WHERE $ip = '67.164.164.165'
```

To which MinSQL will reply only with the matched lines
```
67.164.164.165 - - [24/Jul/2017:00:16:46 +0000] "GET /info.php HTTP/1.1" 200 24564 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
67.164.164.165 - - [24/Jul/2017:00:16:48 +0000] "GET /favicon.ico HTTP/1.1" 404 209 "http://104.236.9.232/info.php" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
```

### By value
You can select log lines that contain a value by using the `LIKE` operator or `NOT NULL` for any entity.

```sql
SELECT * FROM mylog WHERE $line LIKE 'Intel' AND $email IS NOT NULL
```

This query would return all the log lines conaining the word `Intel` that also contain an email address.

## Entities
A list of supported entities by MinSQL :

* *$line*: Represents the whole log line
* *$ip*: Selects any format of ipv4
* *$date*: Any format of date containing date, month and year.
* *$email*: Any email@address.com
* *$quoted*: any text that is within single quotes (') or double quotes (")
* *$url*: any url starting with http
