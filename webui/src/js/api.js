export class API {
  getQueryResults(sql) {
    const url = `/api/`
    return new Promise((resolve, reject) => {
      let formData = new FormData()
      formData.append("sql", sql)
      fetch(url, {
        method: "POST",
        body: formData
      })
        .then(res => {
          var decoder = new TextDecoder()
          var reader = res.body.getReader()
          var data = []
          reader.read().then(function readData(result) {
            const value = decoder.decode(result.value, { stream: true })

            if (res.status && res.status !== 200) {
              reject(value)
              return
            }

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
