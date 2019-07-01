> NOTE: This project is under development, please do not depend on it yet as things may break.

# MinSQL

MinSQL is a log search engine designed with simplicity in mind to the extent that no SDK is needed to interact with it, most programming languages and tools have some form of http request capability (ie: curl) and that's all you need to interact with MinSQL. 

## To build

### Build dependencies:

Make sure you have installed
 - `OpenSSL`:
    - Debian/Ubuntu: `sudo apt install libssl-dev`
    - RedHat/Fedora: `sudo dnf install openssl-devel`
 - `pkg-config`:
    - Debian/Ubuntu: `sudo apt install pkg-config`
    - RedHat/Fedora: `sudo dnf install pkgconfig`
 - `hyperscan-dev`:  
    - Debian/Ubuntu: `sudo apt install libhyperscan-dev`
    - RedHat/Fedora: `sudo dnf install hyperscan`

To build the project simply run 
```bash
cargo build --release
```

## Running the project

An instance of [MinIO](https://github.com/minio/minio) is needed as the storage engine for MinSQL.

To run the project you may specify a `.toml` configuration file, if none is specified *MinSQL* will attempt to look for a `config.toml` file.
```bash
 minsql config.toml
```
A sample configuration file can be found at `config.toml.template`

### Meta Bucket

A bucket to store shared configuration across multiple `MinSQL` instances is needed, the location and access to it should be configured on the `config.toml`.

````toml
[server]
   ...
   metadata_bucket = "minsql-meta"
   metadata_endpoint = "http://localhost:9000"
   access_key = "minio"
   secret_key = "minio123"
````

# Storing logs
For a log `mylog` defined on the `config.toml` we can store logs on *MinSQL* by performing a `PUT` to your `MinSQL` instance

```bash
curl -X PUT \
  http://127.0.0.1:9999/mylog/store \
  -H 'MINSQL-TOKEN: TOKEN1' \
  -d '10.8.0.1 - - [16/May/2019:23:02:56 +0000] "GET / HTTP/1.1" 400 256 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:52.0) Gecko/20100101 Firefox/52.0"'
```

You can send multiple log lines separated by `new line`

# Querying logs

To get data out of MinSQL you can use SQL. Note data MinSQL is a data layer and not a computation layer, therefore certain SQL statements that need computations (SUM, MAX, GROUP BY, JOIN, etc...) are not supported.

All the query statements must be sent via `POST` to your MinSQL instance.

# SELECT
To select all the logs for a particular log you can perform a simple SELECT statement
 ```sql
 SELECT * FROM mylog
 ```
 And send that to MinSQL via POST
 ```bash
curl -X POST \
  http://127.0.0.1:9999/search \
  -H 'MINSQL-TOKEN: TOKEN1' \
  -d 'SELECT * FROM mylog'
```

This will return you all the raw log lines stored for that log.
```bash
67.164.164.165 - - [24/Jul/2017:00:16:46 +0000] "GET /info.php HTTP/1.1" 200 24564 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
67.164.164.165 - - [24/Jul/2017:00:16:48 +0000] "GET /favicon.ico HTTP/1.1" 404 209 "http://104.236.9.232/info.php" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
24.26.204.22 - - [24/Jul/2017:00:17:16 +0000] "GET /info.php HTTP/1.1" 200 24579 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
45.23.126.92 - - [24/Jul/2017:00:16:18 +0000] "GET /info.php HTTP/1.1" 200 24589 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
```

## Select parts of the data
We can get only parts of the data by using any of the supported MinSQL entities, which start with a `$` sign.
### Positional 
We can select from the data by it's position, for example to get the first column and the fourth we can use `$1` and `$4`
```sql
SELECT $1, $4 FROM mylog;
```
To which MinSQL will reply 
```bash
67.164.164.165 [24/Jul/2017:00:16:46 
67.164.164.165 [24/Jul/2017:00:16:48
24.26.204.22 [24/Jul/2017:00:17:16
45.23.126.92 [24/Jul/2017:00:16:18
```

You can see that the data was selected as is, however the selected date column is not clean enough, MinSQL provides other entities to deal with this.

### Select by type

MinSQL provides a nice list of entities that make the extraction of chunks data from your raw data easy thanks to our powerful Schema on Read approach. For example we can select any ip in our data by using the entity `$ip` and any date using `$date`.
```sql
SELECT $ip, $date FROM mylog
```
To which MinSQL will reply
```bash
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

# Filtering
Using the powerful select engine of MinSQL you can also filter the data so only the relevant information that you need to extract from your logs is returned.

For example, to filter out a single ip from your logs you could select by `$ip`

```sql
SELECT * FROM mylog WHERE $ip = '67.164.164.165'
```

To which MinSQL will reply only with the matched lines

```bash
67.164.164.165 - - [24/Jul/2017:00:16:46 +0000] "GET /info.php HTTP/1.1" 200 24564 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
67.164.164.165 - - [24/Jul/2017:00:16:48 +0000] "GET /favicon.ico HTTP/1.1" 404 209 "http://104.236.9.232/info.php" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
```

## Contains a value

You can select log lines that contain a value by using the `LIKE` operator or `NOT NULL` for any entity.

```sql
SELECT * FROM mylog WHERE $line LIKE 'Intel' AND $email IS NOT NULL
```

This query would return all the log lines conaining the word `Intel` that also contain an email address.

# Entities

A list of supported entities by MinSQL:

* *$line*: Represents the whole log line
* *$ip*: Selects any format of ipv4
* *$date*: Any format of date containing date, month and year.
* *$email*: Any email@address.com
* *$quoted*: any text that is withing single quotes (') or double quotes (")
* *$url*: any url starting with http
