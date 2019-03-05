import React from "react"

export const Results = ({ items }) => {
  if (items.length === 0) {
    return <p>There are no results</p>
  }
  const keys = Object.keys(items[0])
  return (
    <table className="table is-fullwidth">
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
        {items.length > 50 && (
          <tr>
            <td>Remaining rows not shown</td>
          </tr>
        )}
      </tbody>
    </table>
  )
}

export default Results
