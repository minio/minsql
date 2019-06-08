// This file is part of the MinSQL
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
use std::fmt;
use std::sync::{Arc, Mutex};

use futures::{future, Future, stream, Stream};
use futures::Sink;
use hyper::{Body, Chunk, Method, Request, Response, StatusCode};

use crate::config::Config;
use crate::ingest::{api_log_store, IngestBuffer};
use crate::query::api_log_search;

//use std::cell::RefCell;
//type ChunkStream = Box<Stream<Item = Chunk, Error = hyper::Error>>;


pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
pub type ResponseFuture = Box<Future<Item=Response<Body>, Error=GenericError> + Send>;


static INDEX: &[u8] = b"MinSQL";
static NOTFOUND: &[u8] = b"Not Found";


#[derive(Debug)]
pub struct RequestedLog {
    pub name: String,
    pub method: String,
}

#[derive(Debug)]
pub struct RequestedLogError {
    details: String
}

impl RequestedLogError {
    pub fn new(msg: &str) -> RequestedLogError {
        RequestedLogError { details: msg.to_string() }
    }
}

impl fmt::Display for RequestedLogError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

pub fn return_404() -> Response<Body> {
    let body = Body::from(NOTFOUND);
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(body)
        .unwrap()
}

// Return 404 not found response.
pub fn return_404_future() -> ResponseFuture {
    Box::new(future::ok(return_404()))
}

pub fn requested_log_from_request(req: &Request<Body>) -> Result<RequestedLog, RequestedLogError> {
    let request_path_no_slash = String::from(&req.uri().path()[1..]);
    let path_split = request_path_no_slash.split("/");
    let parts: Vec<&str> = path_split.collect();
    if parts.len() != 2 {
        return Err(RequestedLogError::new("Invalid log structure"));
    }
    let logname = parts[0].to_string();
    let method = parts[1].to_string();
    return Ok(RequestedLog { name: logname, method: method });
}

// Requests url and returns a future of "{uri}: {status code}"
//fn fetch_file(file: String) -> Box<Future<Item = String, Error = hyper::Error>> {
//    println!("fetching {:?}...", file);
//    let file2 = file.clone();
////    Box::new(client.get(uri).map(move |res| format!("{:?}: {:?}", uri2, res.status())))
//    Box::new(futures::future::ok(file2))
//}


pub fn request_router(req: Request<Body>, cfg: &'static Config, log_ingest_buffers: Arc<HashMap<String, Mutex<IngestBuffer>>>) -> ResponseFuture {
    // handle GETs as their own thing
    if req.method() == &Method::GET {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/test") => {
                let (tx, body) = hyper::Body::channel();

                hyper::rt::spawn({
                    stream::iter_ok(0..10).fold(tx, |tx, i| {
                        tx.send(Chunk::from(format!("Message {} from spawned task", i)))
                            .map_err(|e| {
                                println!("error = {:?}", e.to_string());
                            })
                    })
                        .map(|_| ()) // Drop tx handle
                });

                Box::new(future::ok(Response::new(body)))
            }
            (&Method::GET, "/") | (&Method::GET, "/index.html") => {
                let body = Body::from(INDEX);
                Box::new(future::ok(Response::new(body)))
            }
            _ => {
                // Return 404 not found response.
                return_404_future()
            }
        }
    } else if req.method() == &Method::POST {
        match (req.method(), req.uri().path()) {
            (&Method::POST, "/search") => {
                api_log_search(&cfg, req)
            }
            _ => {
                // Return 404 not found response.
                return_404_future()
            }
        }
    } else {
        //request path without the /
        let requested_log = match requested_log_from_request(&req) {
            Ok(ln) => ln,
            Err(e) => {
                error!("Failed to load configuration: {}", e);
                return return_404_future();
            }
        };

        // is this a valid requested_log? else reject
        match cfg.get_log(&requested_log.name) {
            Some(_) => {} // if we get a log it's valid
            _ => {
                info!("Attemped access of unknow log {}", requested_log.name);
                return return_404_future();
            }
        }

        match (req.method(), &requested_log.method[..]) {
            (&Method::PUT, "store") => {
                api_log_store(cfg, req, log_ingest_buffers)
            }
            _ => {
                // Return 404 not found response.
                return return_404_future();
            }
        }
    }
}




