# MinSQL

MinSQL is a log search engine designed with simplicity in mind to the extend that no SDK is needed to interact with it, most programming languages and tools have some form of http request capability (ie: curl) and that's all you need to interact with minSQL. 

## To build

To build the project simply run 
```bash
cargo build
```

## Running the project
To run the project you may specify a `.toml` configuration file, if none is specified *minSQL* will attempt to look for a `config.toml` file.
```bash
 minsql config.toml
```
A sample configuration file can be found at `config.toml.template`

# Storing logs
For a log `mylog` defined on the `config.toml` we can store logs on *minSQL* by performing a `PUT` to our `minSQL` instance

```bash
curl -X PUT \
  http://127.0.0.1:9999/mylog/store \
  -H 'Content-Type: application/json' \
  -d '10.8.0.1 - - [16/May/2019:23:02:56 +0000] "GET / HTTP/1.1" 400 256 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:52.0) Gecko/20100101 Firefox/52.0"
2019/05/16 23:02:56 [info] 6#6: *7398 client sent plain HTTP request to HTTPS port while reading client request headers, client: 10.8.0.1, server: , request: "GET / HTTP/1.1", host: "35.226.0.43:443"'
```

You can send multiple log lines separated by `new line`

# Querying logs

To get data out of minSQL you can use SQL. Note data minSQL is a data layer and not a computation layer, therefore certain SQL statements that need computations (SUM, MAX, GROUP BY, JOIN, etc...) are not supported.

All the query statements must be sent via `POST` to your minsql instance.

# SELECT
To select all the logs for a particular log you can perform a simple SELECT statement
 ```sql
 SELECT * FROM mylog
 ```
 And send that to minSQL via POST
 ```bash
curl -X POST \
  http://127.0.0.1:9999/search \
  -d 'SELECT * FROM mylog'
```

This will return you all the raw log lines stored for that log.
```bash
67.164.164.165 - - [24/Jul/2017:00:16:46 +0000] "GET /info.php HTTP/1.1" 200 24564 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
67.164.164.165 - - [24/Jul/2017:00:16:48 +0000] "GET /favicon.ico HTTP/1.1" 404 209 "http://104.236.9.232/info.php" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
24.26.204.22 - - [24/Jul/2017:00:17:16 +0000] "GET /info.php HTTP/1.1" 200 24579 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
45.23.126.92 - - [24/Jul/2017:00:16:18 +0000] "GET /info.php HTTP/1.1" 200 24589 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
```

You can select from multiple logs at the same time by separating the queries with semicolon (;), ie:
 ```bash
curl -X POST \
  http://127.0.0.1:9999/search \
  -H 'Content-Type: application/json' \
  -d 'SELECT * FROM mylog;SELECT * FROM otherlog'
```
This will return result of the first query first and then start streaming the second query.

## Select parts of the data
We can get only parts of the data by using any of the supported minSQL entities, which start with a `$` sign.
### Positional 
We can select from the data by it's position, for example to get the first column and the fourth we can use `$1` and `$4`
```sql
SELECT $1, $4 FROM mylog;
```
To which minSQL will reply 
```bash
67.164.164.165 [24/Jul/2017:00:16:46 
67.164.164.165 [24/Jul/2017:00:16:48
24.26.204.22 [24/Jul/2017:00:17:16
45.23.126.92 [24/Jul/2017:00:16:18
```

You can see that the data was selected as is, however the selected date column is not clean enough, minSQL provides other entities to deal with this.

### Select by type

MinSQL provides a nice list of entities that make the extractiong of data from your data easy. For example we can select any ip in our data by using`$ip` and any date using `$date`.
```sql
SELECT $ip, $date FROM mylog
```
To which minSQL will reply
```bash
67.164.164.165 24/Jul/2017
67.164.164.165 24/Jul/2017
24.26.204.22 24/Jul/2017
45.23.126.92 24/Jul/2017
```

# Filtering
Using the powerful select engine of minSQL you can also filter the data so only the relevant information that you need to extract from your logs is returned.

For example, to filter out a single ip from your logs you could select by `$ip`

```sql
SELECT * FROM mylog WHERE $ip = '67.164.164.165'
```

To which minSQL will reply only with the matched lines

```bash
67.164.164.165 - - [24/Jul/2017:00:16:46 +0000] "GET /info.php HTTP/1.1" 200 24564 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
67.164.164.165 - - [24/Jul/2017:00:16:48 +0000] "GET /favicon.ico HTTP/1.1" 404 209 "http://104.236.9.232/info.php" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
```

# Entities

A list of supported entities by minSQL:

* *$ip*: Selects any format of ipv4
* *$date*: Any format of date containing date, month and year.
* *$email*: Any email@address.com
* *$quotedtext*: any text that is withing single quotes (') or double quotes (")
* *$url*: any url starting with http