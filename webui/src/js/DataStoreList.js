import React, { useEffect, useState } from "react"
import api from "./api"

export const DataStoreList = ({ showAdd, closeModal }) => {
  const [datastores, setDataStores] = useState([])
  useEffect(() => {
    api.getDatastores().then(stores => setDataStores(stores))
  }, [])
  return (
    <>
      <header className="modal-card-head">
        <p className="modal-card-title">Data stores</p>
        <button className="delete" aria-label="close" onClick={closeModal} />
      </header>
      <section className="modal-card-body">
        <table className="table is-fullwidth is-hoverable">
          <tbody>
            {Object.keys(datastores).map(ds => {
              const dsInfo = datastores[ds]
              return (
                <tr key={ds}>
                  <td>{ds}</td>
                  <td>{dsInfo.endpoint}</td>
                  <td>{dsInfo.bucket}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </section>
      <footer className="modal-card-foot">
        <button className="button is-primary" onClick={showAdd}>
          Add Store
        </button>
        <button className="button" onClick={closeModal}>
          Cancel
        </button>
      </footer>
    </>
  )
}

export default DataStoreList
