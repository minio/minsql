import React, { useState } from "react"
import Menu from "./Menu"
import Tables from "./Tables"

export const Manage = () => {
  const [showTables, setShowTables] = useState(false)
  return (
    <>
      <Menu showTables={() => setShowTables(true)} />
      {showTables && <Tables closeModal={() => setShowTables(false)} />}
    </>
  )
}

export default Manage
