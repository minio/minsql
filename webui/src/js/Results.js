import React from "react"

export const Results = ({ items }) => {
  if (items.length === 0) {
    return <p>There are no results</p>
  }
  const rows = items.split("\n")
  return (
    <div>
      {rows.map(r => (
        <div>{r}</div>
      ))}
    </div>
  )
}

export default Results
