import request from "superagent"

export class API {
  getQueryResults(sql, table) {
    const url = `/api/`
    return request
      .get(url)
      .query({ sql: sql })
      .then(res => res.body || res.text)
  }
}

const api = new API()
export default api
