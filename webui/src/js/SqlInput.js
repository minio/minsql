import React, { useState, useEffect, useRef } from "react"
import classNames from "classnames"

export const SqlInput = ({ sql: initSql, submitQuery, isBanner }) => {
  const [sql, setSql] = useState(initSql || "")

  const sqlInput = useRef(undefined)

  useEffect(() => {
    sqlInput.current.focus()
  }, [])

  function submit(e) {
    e.preventDefault()
    if (sql) {
      submitQuery(sql)
    }
  }
  return (
    <form onSubmit={submit} className="query__form">
      <div className="field">
        <div className="control has-icons-right">
          <input
            ref={sqlInput}
            className={classNames({
              input: true,
              "is-rounded": true,
              query__input: true,
              "is-large": isBanner
            })}
            type="text"
            placeholder="Enter a SQL query"
            value={sql}
            onChange={e => setSql(e.target.value)}
          />
          <span className="icon is-right">
            <svg
              class="feather feather-search sc-dnqmqq cUQkap"
              xmlns="http://www.w3.org/2000/svg"
              width="24"
              height="24"
              viewBox="0 0 24 24"
              fill="none"
              stroke="#dbdbdb"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
              aria-hidden="true"
              data-reactid="30"
            >
              <circle cx="11" cy="11" r="8" />
              <line x1="21" y1="21" x2="16.65" y2="16.65" />
            </svg>
          </span>
        </div>
      </div>
    </form>
  )
}

export default SqlInput
