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
use std::fmt;

use futures::{future, Future, Stream};
use hyper::{Body, Chunk, Client, header, Method, Request, Response, StatusCode};
use hyper::client::HttpConnector;

use crate::config::Config;
use crate::storage::write_to_datastore;

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
pub type ResponseFuture = Box<Future<Item=Response<Body>, Error=GenericError> + Send>;

static URL: &str = "http://127.0.0.1:1337/json_api";
static POST_DATA: &str = r#"{"original": "data"}"#;


static INDEX: &[u8] = b"MinSQL";
static NOTFOUND: &[u8] = b"Not Found";

#[derive(Debug)]
struct RequestedLog {
    log: String,
    method: String,
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

// Return 404 not found response.
fn return_404() -> ResponseFuture {
    let body = Body::from(NOTFOUND);
    Box::new(future::ok(Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(body)
        .unwrap()))
}

fn requested_log_from_request(req: &Request<Body>) -> Result<RequestedLog, RequestedLogError> {
    let request_path_no_slash = String::from(&req.uri().path()[1..]);
    let path_split = request_path_no_slash.split("/");
    let parts: Vec<&str> = path_split.collect();
    if parts.len() != 2 {
        return Err(RequestedLogError::new("Invalid log structure"));
    }
    let logname = parts[0].to_string();
    let method = parts[1].to_string();
    return Ok(RequestedLog { log: logname, method: method });
}

pub fn request_router(req: Request<Body>, client: &Client<HttpConnector>, cfg: &Config) -> ResponseFuture {
    // handle GETs as their own thing
    if req.method() == &Method::GET {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/") | (&Method::GET, "/index.html") => {
                let body = Body::from(INDEX);
                Box::new(future::ok(Response::new(body)))
            }
            (&Method::GET, "/test.html") => {
                client_request_response(client)
            }
            _ => {
                // Return 404 not found response.
                return_404()
            }
        }
    } else {
        //request path without the /
        let logname = match requested_log_from_request(&req) {
            Ok(ln) => ln,
            Err(e) => {
                error!("Failed to load configuration: {}", e);
                return return_404();
            }
        };

        // is this a valid logname? else reject
        let mut found = false;
        for log in &cfg.log {
            if log.name == logname.log {
                found = true;
            }
        }
        if found == false {
            return return_404();
        }


        match (req.method(), &logname.method[..]) {
            (&Method::POST, "search") => {
                api_post_response(req)
            }
            (&Method::PUT, "store") => {
                api_log_put_response(cfg, req)
            }
            _ => {
                // Return 404 not found response.
                return return_404();
            }
        }
    }
}

fn api_log_put_response(cfg: &Config, req: Request<Body>) -> ResponseFuture {
//    info!("Logging data");
    let requested_log = match requested_log_from_request(&req) {
        Ok(ln) => ln,
        Err(e) => {
            error!("{}", e);
            return return_404();
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
            match write_to_datastore(&requested_log.log, &cfg.datastore[0], &payload) {
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
        })
    )
}

fn client_request_response(client: &Client<HttpConnector>) -> ResponseFuture {
    let req = Request::builder()
        .method(Method::POST)
        .uri(URL)
        .header(header::CONTENT_TYPE, "application/json")
        .body(POST_DATA.into())
        .unwrap();

    Box::new(client.request(req).from_err().map(|web_res| {
        // Compare the JSON we sent (before) with what we received (after):
        let body = Body::wrap_stream(web_res.into_body().map(|b| {
            Chunk::from(format!("<b>POST request body</b>: {}<br><b>Response</b>: {}",
                                POST_DATA,
                                std::str::from_utf8(&b).unwrap()))
        }));

        Response::new(body)
    }))
}

fn api_post_response(req: Request<Body>) -> ResponseFuture {
    // A web api to run against
    Box::new(req.into_body()
        .concat2() // Concatenate all chunks in the body
        .from_err()
        .and_then(|entire_body| {
            // TODO: Replace all unwraps with proper error handling
            let str = String::from_utf8(entire_body.to_vec())?;
            let mut data: serde_json::Value = serde_json::from_str(&str)?;
            data["test"] = serde_json::Value::from("test_value");
            let json = serde_json::to_string(&data)?;
            let response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json))?;
            Ok(response)
        })
    )
}

