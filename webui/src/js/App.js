import React, { Component } from "react"
import api from "./api"
import SqlInput from "./SqlInput"
import Results from "./Results"
import Manage from "./Manage"

class App extends Component {
  constructor(props) {
    super(props)
    this.state = {
      sql: "",
      error: "",
      results: [],
      fetching: false,
      banner: true
    }
    this.submitQuery = this.submitQuery.bind(this)
  }
  submitQuery(sql) {
    this.setState({
      sql: sql,
      banner: false,
      error: "",
      fetching: true
    })
    api
      .getQueryResults(sql)
      .then(res => {
        if (res && res.length > 0) {
          this.setState({
            results: res,
            fetching: false
          })
        } else {
          this.setState({
            results: [],
            error: "There is no response",
            fetching: false
          })
        }
      })
      .catch(err =>
        this.setState({ results: [], error: err.toString(), fetching: false })
      )
  }
  render() {
    return (
      <>
        <Manage />
        {this.state.banner && (
          <section className="hero is-fullheight-with-navbar">
            <div className="hero-body">
              <div className="container">
                <div className="query__banner">
                  <h1 className="title">MinSQL</h1>
                  <SqlInput
                    submitQuery={this.submitQuery}
                    isBanner={this.state.banner}
                  />
                </div>
              </div>
            </div>
          </section>
        )}
        {!this.state.banner && (
          <>
            <nav className="navbar has-shadow is-fixed-top">
              <div className="navbar-brand">
                <a href="/" className="navbar-item">
                  <h1 className="title">MinSQL</h1>
                </a>
              </div>
              <div className="navbar-item">
                <SqlInput sql={this.state.sql} submitQuery={this.submitQuery} />
              </div>
            </nav>
            {this.state.fetching ? (
              <div className="loading" />
            ) : (
              <section className="section results">
                {this.state.error && (
                  <div className="notification is-danger">
                    {this.state.error}
                  </div>
                )}

                {this.state.results.length > 0 && (
                  <Results items={this.state.results} sql={this.state.sql} />
                )}
              </section>
            )}
          </>
        )}
      </>
    )
  }
}

export default App
