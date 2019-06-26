// This file is part of MinSQL
// Copyright (c) 2019 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;
use std::mem;
use std::sync::Mutex;
use std::sync::{Arc, RwLock};

use futures::future::Either;
use futures::{Future, Stream};
use hyper::header;
use hyper::Body;
use hyper::Request;
use hyper::Response;
use hyper::StatusCode;
use log::{error, info};

use crate::config::Config;
use crate::http::ResponseFuture;
use crate::storage::{write_to_datastore, PutObjectError, StorageError};

#[derive(Debug)]
pub struct IngestBuffer {
    total_bytes: u64,
    data: Vec<String>,
}

impl IngestBuffer {
    pub fn new() -> IngestBuffer {
        IngestBuffer {
            total_bytes: 0,
            data: Vec::new(),
        }
    }
}

pub struct Ingest {
    config: Arc<RwLock<Config>>,
}

impl Ingest {
    pub fn new(cfg: Arc<RwLock<Config>>) -> Ingest {
        Ingest { config: cfg }
    }

    /// Handles a PUT operation to a log
    pub fn api_log_store(
        &self,
        req: Request<Body>,
        log_ingest_buffers: Arc<HashMap<String, Mutex<IngestBuffer>>>,
        requested_log: String,
    ) -> ResponseFuture {
        let locked_cfg = Arc::clone(&self.config);
        let flush_cfg = Arc::clone(&self.config);

        // make a clone of the config for the closure
        let cfg = Arc::clone(&self.config);
        let ingest_c = Ingest::new(cfg);
        Box::new(
            req.into_body()
                .concat2() // Concatenate all chunks in the body
                .from_err()
                .and_then(move |entire_body| {
                    // Read the body from the request
                    let payload: String = match String::from_utf8(entire_body.to_vec()) {
                        Ok(str) => str,
                        Err(err) => panic!("Couldn't convert buffer to string: {}", err),
                    };
                    let cfg = locked_cfg.read().unwrap();
                    let log = cfg.get_log(&requested_log).unwrap();
                    // if the commit window is 0s, commit immediately
                    if log.commit_window == "0" {
                        let cfg = Arc::clone(&ingest_c.config);
                        let response_body = write_to_datastore(cfg, &requested_log, &payload).then(
                            |res| -> Result<Response<Body>, _> {
                                match res {
                                    Ok(true) => {
                                        // Send response that the request has been received successfully
                                        let response = Response::builder()
                                            .status(StatusCode::OK)
                                            .header(header::CONTENT_TYPE, "text/plain")
                                            .body(Body::from("ok"))
                                            .unwrap();
                                        Ok(response)
                                    }
                                    Ok(false) => {
                                        let response = Response::builder()
                                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                                            .header(header::CONTENT_TYPE, "text/plain")
                                            .body(Body::from("fail"))
                                            .unwrap();
                                        Ok(response)
                                    }
                                    Err(e) => {
                                        error!("{:?}", e);
                                        let response = Response::builder()
                                            .status(StatusCode::INSUFFICIENT_STORAGE)
                                            .header(header::CONTENT_TYPE, "text/plain")
                                            .body(Body::from("fail"))
                                            .unwrap();
                                        Ok(response)
                                    }
                                }
                            },
                        );
                        Either::A(response_body)
                    } else {
                        // buffer the message
                        let log_name = log.name.clone().unwrap();
                        let ingest_buffer = log_ingest_buffers.get(&log_name[..]).unwrap();
                        let mut protected_data = ingest_buffer.lock().unwrap();
                        protected_data.total_bytes += payload.len() as u64;
                        protected_data.data.push(payload.clone());
                        let total_bytes = protected_data.total_bytes.clone();
                        drop(protected_data);
                        // if we are above storage threshold, we will flush the data
                        if total_bytes > 5 * 1024 * 1024 {
                            info!("Buffer above 5MB, flushing.");
                            let cfg = Arc::clone(&flush_cfg);
                            let ingest_c = Ingest::new(cfg);
                            hyper::rt::spawn({
                                ingest_c
                                    .flush_buffer(&log_name, log_ingest_buffers)
                                    .map(|_| ())
                                    .map_err(|_| ())
                            });
                        }

                        let response = Response::builder()
                            .status(StatusCode::OK)
                            .header(header::CONTENT_TYPE, "text/plain")
                            .body(Body::from("ok."))
                            .unwrap();
                        Either::B(futures::future::ok(response))
                        //                        Ok(response)
                    }
                }),
        )
    }

    /// Flushes an `IngestBuffer` for a given `log_name`
    pub fn flush_buffer(
        &self,
        log_name: &String,
        ingest_buffers: Arc<HashMap<String, Mutex<IngestBuffer>>>,
    ) -> impl Future<Item = bool, Error = StorageError<PutObjectError>> {
        let ingest_buffer = ingest_buffers.get(&log_name[..]).unwrap();
        let mut flushed_data: Vec<String> = Vec::new();
        // lock the ingest_buffer and access it's protected data.s
        let mut protected_data = ingest_buffer.lock().unwrap();
        if protected_data.total_bytes > 0 {
            mem::swap(&mut protected_data.data, &mut flushed_data);
            protected_data.total_bytes = 0;
        }
        drop(protected_data);
        if flushed_data.len() > 0 {
            // Write the data to object storage
            let payload = flushed_data.join("");
            let cfg = Arc::clone(&self.config);
            let res = write_to_datastore(cfg, &log_name, &payload).then(|we| {
                match &we {
                    Ok(_) => (),
                    Err(e) => {
                        error!("Problem flushing data out!! {:?}", e);
                    }
                };
                we
            });
            //TODO: Remove this line later on
            info!("Flushing {}: {} lines", &log_name, flushed_data.len());
            Either::A(res)
        } else {
            Either::B(futures::future::ok(true))
        }
    }
}
