import React, { useState, useEffect, useRef } from "react"
import classNames from "classnames"
import api from "./api"
import getCaretCoordinates from "textarea-caret"
import Downshift from "downshift"
import XRegExp from "xregexp"

const SearchIcon = () => (
  <span className="icon is-right">
    <svg
      className="feather feather-search sc-dnqmqq cUQkap"
      xmlns="http://www.w3.org/2000/svg"
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      stroke="#dbdbdb"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      data-reactid="30"
    >
      <circle cx="11" cy="11" r="8" />
      <line x1="21" y1="21" x2="16.65" y2="16.65" />
    </svg>
  </span>
)

const sqlRE = XRegExp(
  `(?<select>select.*?from +)(?<table>[a-zA-Z0-9_]*)(?<rest>.*)`,
  "gi"
)

export const SqlInput = ({ sql: initSql, submitQuery, isBanner }) => {
  const [sql, setSql] = useState(initSql || "")

  const sqlInput = useRef(undefined)
  useEffect(() => {
    sqlInput.current.focus()
  }, [])

  const [tables, setTables] = useState([])
  useEffect(() => {
    api.getTables().then(res => {
      setTables(Object.keys(res))
    })
  }, [])

  const [caretPos, setCaretPos] = useState()
  const [showSuggestions, setShowSuggestions] = useState(false)
  const [tableName, setTableName] = useState("")

  function onKeyPressed(e) {
    if (e.key === "ArrowUp" || e.key === "ArrowDown") {
      return
    } else if (e.key === "Enter") {
      const filteredTables = tables.filter(t => t.startsWith(tableName))
      setShowSuggestions(false)
      if (filteredTables.length === 0) {
        submit()
      }
      return
    } else if (e.key === "Escape") {
      setShowSuggestions(false)
      return
    }

    setCaretPos(
      getCaretCoordinates(sqlInput.current, sqlInput.current.selectionEnd)
    )

    setShowSuggestions(false)
    setTableName("")

    const reRes = XRegExp.exec(sql, sqlRE)
    if (reRes !== null) {
      if (tableName === "" || tableName !== reRes.table) {
        setShowSuggestions(true)
      } else {
        setShowSuggestions(false)
      }
      setTableName(reRes.table)
    }
  }

  function suggestionSelected(selection) {
    const newSql = XRegExp.replace(sql, sqlRE, match => {
      return `${match.select}${selection}${match.rest}`
    })
    setSql(newSql)
    setShowSuggestions(false)
  }

  function submit(e) {
    e && e.preventDefault()
    if (sql) {
      submitQuery(sql)
    }
  }

  const filteredTables = tables.filter(t => t.startsWith(tableName))

  return (
    <form onSubmit={submit} className="query__form">
      <div className="field">
        <Downshift onSelect={suggestionSelected} defaultHighlightedIndex={0}>
          {({
            getInputProps,
            getItemProps,
            getMenuProps,
            highlightedIndex
          }) => (
            <div>
              <div className="control has-icons-right">
                <input
                  {...getInputProps({
                    ref: sqlInput,
                    className: classNames({
                      input: true,
                      "is-rounded": true,
                      query__input: true,
                      "is-large": isBanner
                    }),
                    type: "text",
                    placeholder: "Enter a SQL query",
                    value: sql,
                    onChange: e => setSql(e.target.value),
                    onKeyUp: onKeyPressed
                  })}
                />
                <SearchIcon />
              </div>
              {showSuggestions && filteredTables.length > 0 && (
                <div
                  {...getMenuProps({
                    className: "dropdown table__suggest is-active"
                  })}
                >
                  <div
                    className="dropdown-menu"
                    id="dropdown-menu"
                    role="menu"
                    style={{ left: caretPos ? caretPos.left + "px" : 0 }}
                  >
                    <div className="dropdown-content">
                      {filteredTables.map((item, index) => (
                        <li
                          {...getItemProps({
                            key: item,
                            index,
                            item,
                            style: {
                              backgroundColor:
                                highlightedIndex === index
                                  ? "lightgray"
                                  : "white"
                            },
                            className: "dropdown-item"
                          })}
                        >
                          {item}
                        </li>
                      ))}
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}
        </Downshift>
      </div>
    </form>
  )
}

export default SqlInput
