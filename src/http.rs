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

use log::info;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use futures::{future, Future};
use hyper::{header, Body, Method, Request, Response, StatusCode};

use crate::auth::token_has_access_to_log;
use crate::config::Config;
use crate::ingest::{api_log_store, IngestBuffer};
use crate::query::api_log_search;

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
pub type ResponseFuture = Box<Future<Item = Response<Body>, Error = GenericError> + Send>;

static INDEX_BODY: &[u8] = b"MinSQL";
static NOTFOUND_BODY: &[u8] = b"Not Found";
static UNAUTHORIZED_BODY: &[u8] = b"Unauthorized";

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
        .header(header::CONTENT_TYPE, "text/plain")
        .body(body)
        .unwrap()
}

pub fn requested_log_from_request(req: &Request<Body>) -> Option<String> {
    let request_path_no_slash = String::from(&req.uri().path()[1..]);
    let path_split = request_path_no_slash.split("/");
    let parts: Vec<&str> = path_split.collect();
    if parts.len() != 2 {
        None
    } else {
        let logname = parts[0].to_string();
        let method = parts[1].to_string();
        if method != "store" {
            None
        } else {
            Some(logname)
        }
    }
}

fn extract_auth_token(req: &Request<Body>, cfg: &Config) -> Result<String, ResponseFuture> {
    match validate_token_from_header(&cfg, &req) {
        HeaderToken::NoToken => Err(Box::new(future::ok(return_401()))),
        HeaderToken::InvalidToken => Err(Box::new(future::ok(return_400("Invalid token")))),
        HeaderToken::Token(tok) => Ok(tok),
    }
}

pub fn request_router(
    req: Request<Body>,
    cfg: Arc<RwLock<Config>>,
    log_ingest_buffers: Arc<HashMap<String, Mutex<IngestBuffer>>>,
) -> ResponseFuture {
    let read_cfg = cfg.read().unwrap();
    let cfg2 = Arc::clone(&cfg);
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            let body = Body::from(INDEX_BODY);
            Box::new(future::ok(Response::new(body)))
        }

        (&Method::POST, "/search") => match extract_auth_token(&req, &read_cfg) {
            Ok(tok) => api_log_search(cfg2, req, &tok),
            Err(err_resp) => err_resp,
        },

        (&Method::PUT, _pth) => {
            match requested_log_from_request(&req) {
                None => Box::new(future::ok(return_404())),
                Some(name) => {
                    // Does log exist in config?
                    if read_cfg.get_log(&name).is_none() {
                        info!("Attempted access of unknown log {}", name);
                        return Box::new(future::ok(return_404()));
                    }

                    let access_token = match extract_auth_token(&req, &read_cfg) {
                        Ok(tok) => tok,
                        Err(err_resp) => return err_resp,
                    };

                    // Does the provided token have access to this log?
                    if !token_has_access_to_log(cfg2, &access_token, &name) {
                        return Box::new(future::ok(return_401()));
                    }
                    let cfg = Arc::clone(&cfg);
                    api_log_store(cfg, req, log_ingest_buffers)
                }
            }
        }

        _ => Box::new(future::ok(return_404())),
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
fn validate_token_from_header(cfg: &Config, req: &Request<Body>) -> HeaderToken {
    let access_key_result = match req.headers().get("MINSQL-TOKEN") {
        Some(val) => val.to_str(),
        None => return HeaderToken::NoToken,
    };
    let access_key = match access_key_result {
        Ok(val) => val,
        Err(_) => return HeaderToken::InvalidToken,
    };
    match cfg.auth.get(access_key) {
        Some(_) => HeaderToken::Token(access_key.to_string()),
        None => HeaderToken::InvalidToken,
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

        Config {
            version: "1".to_string(),
            server: None,
            datastore: HashMap::new(),
            log: HashMap::new(),
            auth: auth,
        }
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
