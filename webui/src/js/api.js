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
          var decoder = new TextDecoder()
          var reader = res.body.getReader()
          var data = []
          reader.read().then(function readData(result) {
            if (result.value === undefined) {
              reject("There are no results")
              return
            }

            const value = decoder.decode(result.value, { stream: true })
            const lines = value.split("\n")
            for (let line of lines) {
              try {
                const parsed = JSON.parse(line)
                data.push(parsed)
              } catch (err) {
                console.log(err)
              }
            }
            resolve(data)
            reader.cancel()
          })
        })
        .catch(err => {
          console.log(err)
          reject(err)
        })
    })
  }
}

const api = new API()
export default api
