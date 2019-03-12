import React, { useState } from "react"
import Progress from "./Progress"

export const Results = ({ items, sql }) => {
  const [downloading, setDownloading] = useState(false)
  const [size, setSize] = useState(0)

  function downloadResults(e) {
    e.preventDefault()
    var xhr = new XMLHttpRequest()
    xhr.open("POST", "/search", true)
    xhr.setRequestHeader("Content-type", "application/x-www-form-urlencoded")
    xhr.responseType = "blob"
    xhr.onprogress = function(event) {
      setSize(event.loaded)
    }
    xhr.onload = function(event) {
      var blob = xhr.response
      var a = document.createElement("a")
      a.href = window.URL.createObjectURL(blob)
      a.download = "results"
      a.dispatchEvent(new MouseEvent("click"))
      setDownloading(false)
    }
    xhr.send(sql)
    setDownloading(true)
    setSize(0)
  }

  if (items.length === 0) {
    return <div className="notification">There are no results</div>
  }

  const keys = Object.keys(items[0])
  return (
    <>
      <div className="box">
        {items.length > 50 && <span>Showing top 50 results, </span>}
        <a href="#download" onClick={downloadResults}>
          Click here
        </a>{" "}
        to download full results.
        {downloading && <Progress size={size} />}
      </div>

      <table className="table is-fullwidth is-hoverable">
        <thead>
          <tr>
            {keys.map(k => (
              <th>{k}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {items.slice(0, 50).map(i => (
            <tr>
              {keys.map(k => (
                <td>{i[k]}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </>
  )
}

export default Results
