import React, { useEffect, useState } from "react"
import api from "./api"

export const TableList = ({ showCreate, closeModal }) => {
  const [tables, setTables] = useState([])
  useEffect(() => {
    api.getTables().then(tables => setTables(tables))
  }, [])
  return (
    <>
      <header className="modal-card-head">
        <p className="modal-card-title">Tables</p>
        <button className="delete" aria-label="close" onClick={closeModal} />
      </header>
      <section className="modal-card-body">
        <table className="table is-fullwidth is-hoverable">
          <tbody>
            {Object.keys(tables).map(table => (
              <tr key={table}>
                <td>{table}</td>
                <td>{tables[table].datastores.join(", ")}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
      <footer className="modal-card-foot">
        <button className="button is-primary" onClick={showCreate}>
          Create table
        </button>
        <button className="button" onClick={closeModal}>
          Cancel
        </button>
      </footer>
    </>
  )
}

export default TableList
