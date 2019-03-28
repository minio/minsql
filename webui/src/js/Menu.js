import React, { useState } from "react"
import OutsideClickHandler from "react-outside-click-handler"

const MenuIcon = () => (
  <svg focusable="false" viewBox="0 0 24 24">
    <path d="M3 18h18v-2H3v2zm0-5h18v-2H3v2zm0-7v2h18V6H3z" />
  </svg>
)

export const Menu = ({ showTables, showDataStores }) => {
  const [showMenu, setShowMenu] = useState(false)

  function onMenuClicked() {
    setShowMenu(showMenu => !showMenu)
  }

  function tablesClicked() {
    setShowMenu(false)
    showTables()
  }

  function dataStoresClicked() {
    setShowMenu(false)
    showDataStores()
  }

  return (
    <OutsideClickHandler onOutsideClick={() => setShowMenu(false)}>
      <div className="dropdown menu is-right is-active">
        <div className="dropdown-trigger">
          <div className="hamburger" role="button" onClick={onMenuClicked}>
            <MenuIcon />
          </div>
        </div>
        {showMenu && (
          <div className="dropdown-menu" id="dropdown-menu3" role="menu">
            <div className="dropdown-content">
              <a className="dropdown-item" onClick={tablesClicked}>
                <h3 className="is-size-6 has-text-weight-semibold">Tables</h3>
                <p>List, Create</p>
              </a>
              <hr className="dropdown-divider" />
              <a className="dropdown-item" onClick={dataStoresClicked}>
                <h3 className="is-size-6 has-text-weight-semibold">
                  Data sources
                </h3>
                <p>List, Add</p>
              </a>
            </div>
          </div>
        )}
      </div>
    </OutsideClickHandler>
  )
}

export default Menu
