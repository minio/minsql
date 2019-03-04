import React, { Component } from "react"
import classNames from "classnames"
import api from "./api"
import Results from "./Results"

class App extends Component {
  constructor(props) {
    super(props)
    this.state = {
      table: "",
      sql: "",
      error: "",
      results: [],
      fetching: false
    }
    this.onSubmit = this.onSubmit.bind(this)
    this.onClear = this.onClear.bind(this)
  }
  onSubmit(e) {
    e.preventDefault()
    const { table, sql } = this.state
    if (!table) {
      this.setState({
        error: "Table cannot be empty"
      })
      return
    }
    this.setState({
      error: "",
      fetching: true
    })
    api
      .getQueryResults(sql, table)
      .then(res => {
        if (res && res.length > 0) {
          this.setState({
            results: res,
            fetching: false
          })
        } else {
          this.setState({ error: "There is no response", fetching: false })
        }
      })
      .catch(err => this.setState({ error: err.toString(), fetching: false }))
  }
  onClear(e) {
    e.preventDefault()
    this.setState({
      sql: "",
      error: "",
      results: []
    })
  }
  render() {
    return (
      <>
        <section className="section">
          <form onSubmit={this.onSubmit}>
            <div className="field">
              <label className="label">SQL Query</label>
              <div className="control">
                <textarea
                  className="textarea"
                  placeholder="Enter a SQL query"
                  value={this.state.sql}
                  onChange={e => this.setState({ sql: e.target.value })}
                />
              </div>
            </div>
            <div className="field">
              <label className="label">Table</label>
              <div className="control">
                <input
                  className="input"
                  type="text"
                  placeholder="Table"
                  value={this.state.table}
                  onChange={e =>
                    this.setState({
                      table: e.target.value
                    })
                  }
                />
              </div>
            </div>
            <div className="field is-grouped">
              <div className="control">
                <button
                  className={classNames({
                    button: true,
                    "is-primary": true,
                    "is-loading": this.state.fetching
                  })}
                >
                  Submit
                </button>
              </div>
              <div className="control">
                <button className="button is-text" onClick={this.onClear}>
                  Clear
                </button>
              </div>
            </div>
          </form>
        </section>
        {this.state.error && (
          <section className="section">
            <div className="notification is-danger">{this.state.error}</div>
          </section>
        )}
        <section className="section">
          {this.state.results.length > 0 && (
            <Results items={this.state.results} />
          )}
        </section>
      </>
    )
  }
}

export default App
