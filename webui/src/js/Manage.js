import React, { useState } from "react"
import Menu from "./Menu"
import Tables from "./Tables"
import DataStores from "./DataStores"

export const Manage = () => {
  const [showTables, setShowTables] = useState(false)
  const [showDataStores, setShowDataStores] = useState(false)
  return (
    <>
      <Menu
        showTables={() => setShowTables(true)}
        showDataStores={() => setShowDataStores(true)}
      />
      {showTables && <Tables closeModal={() => setShowTables(false)} />}
      {showDataStores && (
        <DataStores closeModal={() => setShowDataStores(false)} />
      )}
    </>
  )
}

export default Manage
