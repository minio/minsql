import React, { useState } from "react"
import TableList from "./TableList"
import TableCreate from "./TableCreate"

export const Tables = ({ closeModal }) => {
  const [showCreate, setShowCreate] = useState(false)
  return (
    <div className="modal is-active">
      <div className="modal-background" />
      <div className="modal-card">
        {showCreate ? (
          <TableCreate
            closeModal={closeModal}
            back={() => setShowCreate(false)}
          />
        ) : (
          <TableList
            closeModal={closeModal}
            showCreate={() => setShowCreate(true)}
          />
        )}
      </div>
    </div>
  )
}

export default Tables
