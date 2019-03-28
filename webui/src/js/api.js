import request from "superagent"

export class API {
  getQueryResults(sql) {
    const url = `/search`
    return new Promise((resolve, reject) => {
      fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded"
        },
        body: sql
      })
        .then(res => {
          if (!res.ok) {
            res.text().then(reject)
            return
          }
          var decoder = new TextDecoder()
          var reader = res.body.getReader()
          var data = []

          function readData() {
            reader
              .read()
              .then(result => {
                if (result.value !== undefined) {
                  const value = decoder.decode(result.value, { stream: true })
                  const lines = value.split("\n").filter(l => l.length > 0)
                  for (let line of lines) {
                    try {
                      const parsed = JSON.parse(line)
                      data.push(parsed)
                    } catch (err) {
                      console.log(err)
                    }
                  }

                  // We will read at most 100 records
                  if (data.length >= 100) {
                    reader.cancel()
                    resolve(data)
                  }
                }

                // If we have reached the end, close the reader and return(resolve) the data
                if (result.done) {
                  reader.cancel()
                  resolve(data)
                } else {
                  // Continue reading
                  readData()
                }
              })
              .catch(err => {
                console.log(err)
                reject(err)
              })
          }

          // Start reading data
          readData()
        })
        .catch(err => {
          console.log(err)
          reject(err)
        })
    })
  }

  getTables() {
    const url = "/ui/listTables"
    return request.get(url).then(res => res.body)
  }

  createTable(table, datastores) {
    const url = "/ui/createTable"
    const data = {
      [table]: {
        datastores: datastores
      }
    }
    return request
      .post(url)
      .send(data)
      .then(res => res.body)
  }

  getDatastores() {
    const url = "/ui/listDataStores"
    return request.get(url).then(res => res.body)
  }

  createDataStore(datastore, endpoint, accessKey, secretKey, bucket, prefix) {
    const url = "/ui/createDataStore"
    const data = {
      [datastore]: {
        endpoint: endpoint,
        access_key: accessKey,
        secret_key: secretKey,
        bucket: bucket,
        prefix: prefix
      }
    }
    return request
      .post(url)
      .send(data)
      .then(res => res.body)
  }
}

const api = new API()
export default api
