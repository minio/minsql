import React, { useState } from "react"
import api from "./api"
import classNames from "classnames"
import { Formik } from "formik"
import * as Yup from "yup"

export const DataStoreAdd = ({ back, closeModal }) => {
  const [serverError, setServerError] = useState("")

  function submit({
    datastore,
    endpoint,
    accessKey,
    secretKey,
    bucket,
    prefix
  }) {
    api
      .createDataStore(
        datastore,
        endpoint,
        accessKey,
        secretKey,
        bucket,
        prefix
      )
      .then(() => {
        back()
      })
      .catch(err => {
        setServerError(err.response.text)
      })
  }
  return (
    <Formik
      initialValues={{
        datastore: "",
        endpoint: "",
        accessKey: "",
        secretKey: "",
        bucket: "",
        prefix: ""
      }}
      onSubmit={(values, { setSubmitting }) => {
        setTimeout(() => {
          submit(values)
          setSubmitting(false)
        }, 500)
      }}
      validationSchema={Yup.object().shape({
        datastore: Yup.string().required("Required"),
        endpoint: Yup.string().required("Required"),
        accessKey: Yup.string().required("Required"),
        secretKey: Yup.string().required("Required"),
        bucket: Yup.string().required("Required")
      })}
    >
      {({
        values,
        touched,
        errors,
        isSubmitting,
        handleChange,
        handleBlur,
        handleSubmit
      }) => (
        <>
          <header className="modal-card-head">
            <p className="modal-card-title">Add Data Store</p>
            <button
              className="delete"
              aria-label="close"
              onClick={closeModal}
            />
          </header>
          <section className="modal-card-body">
            <form onSubmit={handleSubmit}>
              <div class="field">
                <label class="label">Data store</label>
                <div class="control">
                  <input
                    id="datastore"
                    class={classNames({
                      input: true,
                      "is-danger": touched.datastore && errors.datastore
                    })}
                    type="text"
                    placeholder="Table name"
                    value={values.datastore}
                    onChange={handleChange}
                  />
                </div>
                {touched.datastore && errors.datastore && (
                  <p class="help is-danger">Data store is required</p>
                )}
              </div>

              <div class="field">
                <label class="label">Endpoint</label>
                <div class="control">
                  <input
                    id="endpoint"
                    class={classNames({
                      input: true,
                      "is-danger": touched.endpoint && errors.endpoint
                    })}
                    type="text"
                    placeholder="Endpoint"
                    value={values.endpoint}
                    onChange={handleChange}
                  />
                </div>
                {touched.endpoint && errors.endpoint && (
                  <p class="help is-danger">Endpoint is required</p>
                )}
              </div>

              <div class="field">
                <label class="label">AccessKey</label>
                <div class="control">
                  <input
                    id="accessKey"
                    class={classNames({
                      input: true,
                      "is-danger": touched.accessKey && errors.accessKey
                    })}
                    type="text"
                    placeholder="AccessKey"
                    value={values.accessKey}
                    onChange={handleChange}
                  />
                </div>
                {touched.accessKey && errors.accessKey && (
                  <p class="help is-danger">AccessKey is required</p>
                )}
              </div>

              <div class="field">
                <label class="label">SecretKey</label>
                <div class="control">
                  <input
                    id="secretKey"
                    class={classNames({
                      input: true,
                      "is-danger": touched.secretKey && errors.secretKey
                    })}
                    type="text"
                    placeholder="SecretKey"
                    value={values.secretKey}
                    onChange={handleChange}
                  />
                </div>
                {touched.secretKey && errors.secretKey && (
                  <p class="help is-danger">SecretKey is required</p>
                )}
              </div>

              <div class="field">
                <label class="label">Bucket</label>
                <div class="control">
                  <input
                    id="bucket"
                    class={classNames({
                      input: true,
                      "is-danger": touched.bucket && errors.bucket
                    })}
                    type="text"
                    placeholder="Bucket"
                    value={values.bucket}
                    onChange={handleChange}
                  />
                </div>
                {touched.bucket && errors.bucket && (
                  <p class="help is-danger">Bucket is required</p>
                )}
              </div>

              <div class="field">
                <label class="label">Prefix(optional)</label>
                <div class="control">
                  <input
                    id="prefix"
                    class={classNames({
                      input: true,
                      "is-danger": touched.prefix && errors.prefix
                    })}
                    type="text"
                    placeholder="Prefix"
                    value={values.prefix}
                    onChange={handleChange}
                  />
                </div>
                {touched.prefix && errors.prefix && (
                  <p class="help is-danger">Prefix is required</p>
                )}
              </div>
            </form>
          </section>
          <footer className="modal-card-foot">
            {serverError && (
              <p class="help is-danger modal-card-foot-error">{serverError}</p>
            )}
            <button
              className={classNames({
                button: true,
                "is-primary": true,
                "is-loading": isSubmitting
              })}
              type="submit"
              onClick={handleSubmit}
            >
              Add
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

export default DataStoreAdd
