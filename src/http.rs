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

use log::{error, info};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use futures::Sink;
use futures::{future, stream, Future, Stream};
use hyper::{Body, Chunk, Method, Request, Response, StatusCode};

use crate::auth::token_has_access_to_log;
use crate::config::Config;
use crate::ingest::{api_log_store, IngestBuffer};
use crate::query::api_log_search;

//use std::cell::RefCell;
//type ChunkStream = Box<Stream<Item = Chunk, Error = hyper::Error>>;

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
pub type ResponseFuture = Box<Future<Item = Response<Body>, Error = GenericError> + Send>;

static INDEX_BODY: &[u8] = b"MinSQL";
static NOTFOUND_BODY: &[u8] = b"Not Found";
static UNAUTHORIZED_BODY: &[u8] = b"Unauthorized";

#[derive(Debug)]
pub struct RequestedLog {
    pub name: String,
    pub method: String,
}

#[derive(Debug)]
pub struct RequestedLogError {
    details: String,
}

impl RequestedLogError {
    pub fn new(msg: &str) -> RequestedLogError {
        RequestedLogError {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for RequestedLogError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

pub fn return_404() -> Response<Body> {
    let body = Body::from(NOTFOUND_BODY);
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(body)
        .unwrap()
}

pub fn return_401() -> Response<Body> {
    let body = Body::from(UNAUTHORIZED_BODY);
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .body(body)
        .unwrap()
}

pub fn return_400(message: &str) -> Response<Body> {
    let body = Body::from(format!("Bad request: {}", &message));
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(body)
        .unwrap()
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
    return Ok(RequestedLog {
        name: logname,
        method: method,
    });
}

pub fn request_router(
    req: Request<Body>,
    cfg: &'static Config,
    log_ingest_buffers: Arc<HashMap<String, Mutex<IngestBuffer>>>,
) -> ResponseFuture {
    // handle GETs as their own thing since they are unauthenticated
    if req.method() == &Method::GET {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/test") => {
                let (tx, body) = hyper::Body::channel();

                hyper::rt::spawn({
                    stream::iter_ok(0..10)
                        .fold(tx, |tx, i| {
                            tx.send(Chunk::from(format!("Message {} from spawned task", i)))
                                .map_err(|e| {
                                    println!("error = {:?}", e.to_string());
                                })
                        })
                        .map(|_| ()) // Drop tx handle
                });

                return Box::new(future::ok(Response::new(body)));
            }
            (&Method::GET, "/") | (&Method::GET, "/index.html") => {
                let body = Body::from(INDEX_BODY);
                return Box::new(future::ok(Response::new(body)));
            }
            _ => {
                // Return 404 not found response.
                return Box::new(future::ok(return_404()));
            }
        }
    }

    // POST and PUT request are authenticated, let's validate ACCESS/SECRET before continuing
    let access_token = match validate_token_from_header(&cfg, &req) {
        HeaderToken::NoToken => return Box::new(future::ok(return_401())),
        HeaderToken::InvalidToken => return Box::new(future::ok(return_400("Invalid token"))),
        HeaderToken::Token(tok) => tok,
    };
    // POST is used for search queries
    if req.method() == &Method::POST {
        match (req.method(), req.uri().path()) {
            (&Method::POST, "/search") => api_log_search(&cfg, req, &access_token),
            _ => {
                // Return 404 not found response.
                return Box::new(future::ok(return_404()));
            }
        }
    } else {
        // Means we got a PUT operation, this can be validated for access immediately
        let requested_log = match requested_log_from_request(&req) {
            Ok(ln) => ln,
            Err(e) => {
                error!("Failed to load configuration: {}", e);
                return Box::new(future::ok(return_404()));
            }
        };

        // is this a valid requested_log? else reject
        match cfg.get_log(&requested_log.name) {
            Some(_) => {} // if we get a log it's valid
            _ => {
                info!("Attemped access of unknow log {}", requested_log.name);
                return Box::new(future::ok(return_404()));
            }
        }

        // does the provided token have access to this log?
        if !token_has_access_to_log(&cfg, &access_token, &requested_log.name) {
            return Box::new(future::ok(return_401()));
        }

        match (req.method(), &requested_log.method[..]) {
            (&Method::PUT, "store") => api_log_store(cfg, req, log_ingest_buffers),
            _ => {
                // Return 404 not found response.
                return Box::new(future::ok(return_404()));
            }
        }
    }
}

/// Represents the presence of a token in the header and whether it can be read as valid ASCII.
#[derive(PartialEq, Debug)]
enum HeaderToken {
    NoToken,
    InvalidToken,
    Token(String),
}

/// Returns a `HeaderToken` with the details regarding the presence/validity of the auth token
/// in the request.
fn validate_token_from_header(cfg: &'static Config, req: &Request<Body>) -> HeaderToken {
    let access_key_result = match req.headers().get("MINSQL-TOKEN") {
        Some(val) => val.to_str(),
        None => return HeaderToken::NoToken,
    };
    let access_key = match access_key_result {
        Ok(val) => val,
        Err(_) => return HeaderToken::InvalidToken,
    };
    match cfg.auth.get(access_key) {
        Some(_) => return HeaderToken::Token(access_key.to_string()),
        None => return HeaderToken::InvalidToken,
    }
}

#[cfg(test)]
mod http_tests {
    use crate::config::LogAuth;

    use super::*;

    // Generates a Config object with only one auth item for one log
    fn get_auth_config_for(token: String, log_name: String) -> Config {
        let mut log_auth_map: HashMap<String, LogAuth> = HashMap::new();
        log_auth_map.insert(
            log_name,
            LogAuth {
                token: token.clone(),
                api: Vec::new(),
                expire: "".to_string(),
                status: "".to_string(),
            },
        );

        let mut auth = HashMap::new();
        auth.insert(token.clone(), log_auth_map);

        let cfg = Config {
            version: "1".to_string(),
            server: None,
            datastore: HashMap::new(),
            log: HashMap::new(),
            auth: auth,
        };
        cfg
    }

    struct ValidTokenHeaderTest {
        valid_token: String,
        valid_log: String,

        method: String,
        headers: Vec<(String, String)>,

        expected: HeaderToken,
        expected_token: Option<String>,
    }

    fn run_test_validate_token_from_header(case: ValidTokenHeaderTest) {
        let cfg = get_auth_config_for(case.valid_token, case.valid_log);
        let cfg = Box::new(cfg);
        let cfg: &'static _ = Box::leak(cfg);

        let req: Request<Body>;

        let mut req2 = Request::builder();
        let mut req2 = req2.method(&case.method[..]);
        for (header, value) in case.headers {
            req2 = req2.header(&header[..], &value[..]);
        }
        req = req2.body(Body::from("test")).unwrap();

        let result = validate_token_from_header(&cfg, &req);
        match case.expected {
            HeaderToken::Token(_) => assert_eq!(
                result,
                HeaderToken::Token(case.expected_token.unwrap_or_else(|| { "".to_string() }))
            ),
            other => assert_eq!(result, other),
        }
    }

    #[test]
    fn valid_token_header() {
        run_test_validate_token_from_header(ValidTokenHeaderTest {
            valid_token: "TOKEN1".to_string(),
            valid_log: "mylog".to_string(),

            method: "PUT".to_string(),
            headers: vec![("MINSQL-TOKEN".to_string(), "TOKEN1".to_string())],

            expected: HeaderToken::Token("TOKEN1".to_string()),
            expected_token: Some("TOKEN1".to_string()),
        })
    }

    #[test]
    fn missing_token_header() {
        run_test_validate_token_from_header(ValidTokenHeaderTest {
            valid_token: "TOKEN1".to_string(),
            valid_log: "mylog".to_string(),

            method: "PUT".to_string(),
            headers: Vec::new(),

            expected: HeaderToken::NoToken,
            expected_token: None,
        })
    }

    #[test]
    fn invalid_token_header() {
        run_test_validate_token_from_header(ValidTokenHeaderTest {
            valid_token: "TOKEN1".to_string(),
            valid_log: "mylog".to_string(),

            method: "PUT".to_string(),
            headers: vec![("MINSQL-TOKEN".to_string(), "TOKEN2".to_string())],

            expected: HeaderToken::InvalidToken,
            expected_token: Some("TOKEN2".to_string()),
        })
    }
}
