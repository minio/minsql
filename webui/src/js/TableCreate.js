import React, { useEffect, useState } from "react"
import api from "./api"
import classNames from "classnames"
import { Formik, FieldArray } from "formik"
import * as Yup from "yup"

export const TableCreate = ({ back, closeModal }) => {
  const [datastores, setDatastores] = useState([])
  useEffect(() => {
    api.getDatastores().then(stores => setDatastores(stores))
  }, [])

  function submit(table, datastores) {
    api.createTable(table, datastores).then(() => {
      back()
    })
  }
  return (
    <Formik
      initialValues={{ table: "", datastores: [] }}
      onSubmit={(values, { setSubmitting }) => {
        setTimeout(() => {
          submit(values.table, values.datastores)
          setSubmitting(false)
        }, 500)
      }}
      validationSchema={Yup.object().shape({
        table: Yup.string().required("Required"),
        datastores: Yup.array().required("Required")
      })}
    >
      {({
        values,
        touched,
        errors,
        dirty,
        isSubmitting,
        handleChange,
        handleBlur,
        handleSubmit,
        setFieldValue
      }) => (
        <>
          <header className="modal-card-head">
            <p className="modal-card-title">Create table</p>
            <button
              className="delete"
              aria-label="close"
              onClick={closeModal}
            />
          </header>
          <section className="modal-card-body">
            <form>
              <div class="field">
                <label class="label">Table name</label>
                <div class="control">
                  <input
                    id="table"
                    class={classNames({
                      input: true,
                      "is-danger": touched.table && errors.table
                    })}
                    type="text"
                    placeholder="Table name"
                    value={values.table}
                    onChange={handleChange}
                  />
                </div>
                {touched.table && errors.table && (
                  <p class="help is-danger">Table name is required</p>
                )}
              </div>
              <div class="field">
                <label class="label">Datastores</label>
                <div class="tags is-marginless">
                  <FieldArray
                    name="datastores"
                    render={arrayHelpers =>
                      Object.keys(datastores).map(datastore => (
                        <a
                          href={`#${datastore}`}
                          key={datastore}
                          role="button"
                          onClick={e => {
                            e.preventDefault()
                            if (values.datastores.includes(datastore)) {
                              const idx = values.datastores.indexOf(datastore)
                              arrayHelpers.remove(idx)
                            } else {
                              arrayHelpers.push(datastore)
                            }
                          }}
                        >
                          <span
                            className={classNames({
                              tag: true,
                              "is-unselectable": true,
                              "is-primary": values.datastores.includes(
                                datastore
                              )
                            })}
                          >
                            {datastore}
                          </span>
                        </a>
                      ))
                    }
                  />
                </div>
                {touched.datastores && errors.datastores && (
                  <p class="help is-danger">
                    At least one data store should be selected
                  </p>
                )}
              </div>
            </form>
          </section>
          <footer className="modal-card-foot">
            <button className="button is-primary" onClick={handleSubmit}>
              Create table
            </button>
            <button className="button" onClick={back}>
              {"< Back"}
            </button>
          </footer>
        </>
      )}
    </Formik>
  )
}

export default TableCreate
