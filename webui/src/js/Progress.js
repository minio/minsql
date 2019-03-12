import React from "react"
import filesize from "filesize"

export const Progress = ({ size }) => {
  return (
    <div className="box progress__bar">
      Downloading {size > 0 && <span>({filesize(size)})</span>}
      <progress className="progress" max="100">
        15%
      </progress>
    </div>
  )
}

export default Progress
