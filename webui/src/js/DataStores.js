import React, { useState } from "react"
import DataStoreList from "./DataStoreList"
import DataStoreAdd from "./DataStoreAdd"

export const DataStores = ({ closeModal }) => {
  const [showAdd, setShowAdd] = useState(false)
  return (
    <div className="modal is-active">
      <div className="modal-background" />
      <div className="modal-card">
        {showAdd ? (
          <DataStoreAdd
            closeModal={closeModal}
            back={() => setShowAdd(false)}
          />
        ) : (
          <DataStoreList
            closeModal={closeModal}
            showAdd={() => setShowAdd(true)}
          />
        )}
      </div>
    </div>
  )
}

export default DataStores
