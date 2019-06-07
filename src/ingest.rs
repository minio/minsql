// MinSQL
// Copyright (C) 2019  MinIO
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
use std::sync::Arc;
use std::sync::Mutex;

use futures::{Future, Stream};
use hyper::Body;
use hyper::header;
use hyper::Request;
use hyper::Response;
use hyper::StatusCode;

use crate::config::Config;
use crate::http::requested_log_from_request;
use crate::http::ResponseFuture;
use crate::http::return_404_future;
use crate::storage::write_to_datastore;

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

pub fn flush_buffer(log_name: &String, cfg: &Config, ingest_buffers: Arc<HashMap<String, Mutex<IngestBuffer>>>) {
    let ingest_buffer = ingest_buffers.get(&log_name[..]).unwrap();
    let mut flushed_data: Vec<String> = Vec::new();
    let mut protected_data = ingest_buffer.lock().unwrap();
    if protected_data.total_bytes > 0 {
        for data_bit in std::mem::replace(&mut protected_data.data, vec![]) {
            flushed_data.push(data_bit);
        }
        protected_data.data.clear();
        protected_data.total_bytes = 0;
    }
    drop(protected_data);
    if flushed_data.len() > 0 {
        // Write the data to object storage
        let payload = flushed_data.join("");
        match write_to_datastore(&log_name, &cfg, &payload) {
            Ok(_) => (),
            Err(e) => {
                error!("Problem flushing data out!! {:?}",e);
            }
        }
        //TODO: Remove this line later on
        info!("Flushing {}: {} lines", &log_name, flushed_data.len());
    }
}

// Handles a PUT operation to a log
pub fn api_log_store(cfg: &Config, req: Request<Body>, log_ingest_buffers: Arc<HashMap<String, Mutex<IngestBuffer>>>) -> ResponseFuture {
    let requested_log = match requested_log_from_request(&req) {
        Ok(ln) => ln,
        Err(e) => {
            error!("{}", e);
            return return_404_future();
        }
    };
    // make a clone of the config for the closure
    let cfg = cfg.clone();
    Box::new(req.into_body()
        .concat2() // Concatenate all chunks in the body
        .from_err()
        .and_then(move |entire_body| {
            // Read the body from the request
            let payload: String = match String::from_utf8(entire_body.to_vec()) {
                Ok(str) => str,
                Err(err) => panic!("Couldn't convert buffer to string: {}", err)
            };

            let log = cfg.get_log(&requested_log.name).unwrap();
            // if the commit window is 0s, commit immediately
            if log.commit_window == "0" {
                match write_to_datastore(&requested_log.name, &cfg, &payload) {
                    Ok(x) => x,
                    Err(e) => {
                        error!("{}", e);
                        let response = Response::builder()
                            .status(StatusCode::INSUFFICIENT_STORAGE)
                            .header(header::CONTENT_TYPE, "text/plain")
                            .body(Body::from("fail"))?;
                        return Ok(response);
                    }
                };

                // Send response that the request has been received successfully
                let response = Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "text/plain")
                    .body(Body::from("ok"))?;
                Ok(response)
            } else {
                // buffer the message
                let ingest_buffer = log_ingest_buffers.get(&log.name[..]).unwrap();
                let mut protected_data = ingest_buffer.lock().unwrap();
                protected_data.total_bytes += payload.len() as u64;
                protected_data.data.push(payload.clone());
                let total_bytes = protected_data.total_bytes.clone();
                drop(protected_data);
                // if we are above storage threshold, we will flush the data
                if total_bytes > 5 * 1024 * 1024 {
                    info!("Buffer above 5MB, flushing.");
                    let log_name = log.name.clone();
                    hyper::rt::spawn({
                        flush_buffer(&log_name, &cfg, log_ingest_buffers);
                        futures::future::ok(())
                    });
                }

                let response = Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "text/plain")
                    .body(Body::from("ok."))?;
                Ok(response)
            }
        })
    )
}