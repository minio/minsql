import React, { useState, useEffect, useRef } from "react"

export const SqlInput = ({ sql: initSql, submitQuery }) => {
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
    <form onSubmit={submit}>
      <div className="field">
        <div className="control">
          <input
            ref={sqlInput}
            className="input is-rounded query__input"
            type="text"
            placeholder="Enter a SQL query"
            value={sql}
            onChange={e => setSql(e.target.value)}
          />
        </div>
      </div>
    </form>
  )
}

export default SqlInput
