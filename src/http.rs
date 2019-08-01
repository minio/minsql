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
use std::ffi::OsStr;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use futures::{future, Future};
use hyper::{Body, Method, Request, Response, StatusCode};
use log::info;
use serde_derive::Serialize;
use std::borrow::Cow;

use crate::api::Api;
use crate::auth::Auth;
use crate::config::Config;
use crate::constants::{APP_JAVASCRIPT, APP_JSON, IMAGE_JPEG, TEXT_HTML, UNKNOWN_CONTENT_TYPE};
use crate::ingest::{Ingest, IngestBuffer};
use crate::query::Query;

#[derive(RustEmbed)]
#[folder = "static/"]
struct Asset;

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
pub type ResponseFuture = Box<Future<Item = Response<Body>, Error = GenericError> + Send>;

static INDEX_BODY: &[u8] = b"MinSQL";
static NOTFOUND_BODY: &str = "Not Found";
static UNAUTHORIZED_BODY: &str = "Unauthorized";

pub struct Http {
    config: Arc<RwLock<Config>>,
}

impl Http {
    pub fn new(cfg: Arc<RwLock<Config>>) -> Http {
        Http { config: cfg }
    }

    pub fn request_router(
        &self,
        req: Request<Body>,
        log_ingest_buffers: Arc<HashMap<String, Mutex<IngestBuffer>>>,
    ) -> ResponseFuture {
        let cfg = self.config.read().unwrap();

        let request_path_no_slash = String::from(&req.uri().path()[1..]);
        // Index 0 indicates wether they want an API
        let parts: Vec<&str> = request_path_no_slash.split("/").collect();

        match (req.method(), req.uri().path(), parts.get(0)) {
            // delegate anything starting with /api/ to the api router
            (_, _, Some(&"ui")) => serve_static_content(req),
            (_, _, Some(&"api")) => {
                let api = Api::new(Arc::clone(&self.config));
                api.router(req, parts)
            }
            (&Method::GET, "/", _) => {
                let body = Body::from(INDEX_BODY);
                Box::new(future::ok(Response::new(body)))
            }

            (&Method::POST, "/search", _) => match self.extract_auth_token(&req) {
                Ok(tok) => {
                    let cfg = Arc::clone(&self.config);
                    let query_c = Query::new(cfg);
                    query_c.api_log_search(req, &tok)
                }
                Err(err_resp) => err_resp,
            },

            (&Method::PUT, _pth, _) => {
                match self.requested_log_from_request(&req) {
                    None => Box::new(future::ok(return_404())),
                    Some(name) => {
                        // Does log exist in config?
                        if cfg.get_log(&name).is_none() {
                            info!("Attempted access of unknown log {}", name);
                            return Box::new(future::ok(return_404()));
                        }

                        let access_token = match self.extract_auth_token(&req) {
                            Ok(tok) => tok,
                            Err(err_resp) => return err_resp,
                        };

                        // Does the provided token have access to this log?
                        let cfg = Arc::clone(&self.config);
                        let auth_c = Auth::new(cfg);
                        if !auth_c.token_has_access_to_log(&access_token, &name) {
                            return Box::new(future::ok(return_401()));
                        }
                        let ingest_c = Ingest::new(Arc::clone(&self.config));
                        ingest_c.api_log_store(req, log_ingest_buffers, name)
                    }
                }
            }

            _ => Box::new(future::ok(return_404())),
        }
    }

    fn extract_auth_token(&self, req: &Request<Body>) -> Result<String, ResponseFuture> {
        match self.validate_token_from_header(&req) {
            HeaderToken::NoToken => Err(Box::new(future::ok(return_401()))),
            HeaderToken::InvalidToken => Err(Box::new(future::ok(return_400("Invalid token")))),
            HeaderToken::Token(tok) => Ok(tok),
        }
    }

    /// Returns a `HeaderToken` with the details regarding the presence/validity of the auth token
    /// in the request.
    pub fn validate_token_from_header(&self, req: &Request<Body>) -> HeaderToken {
        let access_key_result = match req.headers().get("MINSQL-TOKEN") {
            Some(val) => val.to_str(),
            None => return HeaderToken::NoToken,
        };
        let access_key = match access_key_result {
            Ok(val) => val,
            Err(_) => return HeaderToken::InvalidToken,
        };
        if access_key.len() != 48 {
            return HeaderToken::InvalidToken;
        }
        let cfg = self.config.read().unwrap();
        match cfg.tokens.get(&access_key[0..16]) {
            Some(token) => {
                if &token.secret_key == &access_key[16..48] {
                    HeaderToken::Token(access_key.to_string())
                } else {
                    HeaderToken::InvalidToken
                }
            }
            None => HeaderToken::InvalidToken,
        }
    }

    pub fn requested_log_from_request(&self, req: &Request<Body>) -> Option<String> {
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
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    message: String,
}

pub fn return_500(message: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from(message.to_string()))
        .unwrap()
}

pub fn return_404() -> Response<Body> {
    let obj = ErrorResponse {
        message: NOTFOUND_BODY.to_string(),
    };
    let output = serde_json::to_string(&obj).unwrap();
    let body = Body::from(output);
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(body)
        .unwrap()
}

pub fn return_401() -> Response<Body> {
    let obj = ErrorResponse {
        message: UNAUTHORIZED_BODY.to_string(),
    };
    let output = serde_json::to_string(&obj).unwrap();
    let body = Body::from(output);
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .body(body)
        .unwrap()
}

pub fn return_400(message: &str) -> Response<Body> {
    let obj = ErrorResponse {
        message: format!("Bad request: {}", &message),
    };
    let output = serde_json::to_string(&obj).unwrap();
    let body = Body::from(output);
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(body)
        .unwrap()
}

/// Represents the presence of a token in the header and whether it can be read as valid ASCII.
#[derive(PartialEq, Debug)]
pub enum HeaderToken {
    NoToken,
    InvalidToken,
    Token(String),
}

/// Serves content from the `static` folder
fn serve_static_content(req: Request<Body>) -> ResponseFuture {
    let mut full_path: String = req.uri().path()[1..].to_string();

    let mut content_type = match Path::new(req.uri().path())
        .extension()
        .and_then(OsStr::to_str)
    {
        Some(ext) => match ext {
            "html" => TEXT_HTML,
            "css" => "text/css",
            "js" => APP_JAVASCRIPT,
            "jsonp" => APP_JAVASCRIPT,
            "json" => APP_JSON,
            "jpeg" => IMAGE_JPEG,
            "jpe" => IMAGE_JPEG,
            "jpg" => IMAGE_JPEG,
            "png" => "image/png",
            "gif" => "image/gif",
            "svg" => "image/svg+xml",
            _ => UNKNOWN_CONTENT_TYPE,
        },
        None => UNKNOWN_CONTENT_TYPE,
    };

    // if they are accessing anything starting with /ui/*/ without extension and not `/ui/assets/`
    // reroute to index.html
    let request_path_no_slash = String::from(&req.uri().path()[1..]);
    let parts: Vec<&str> = request_path_no_slash.split("/").collect();

    // append index.html to path. if the path ends up with `ui/`
    if (full_path.len() >= 2 && full_path[full_path.len() - 2..] == *"ui")
        || (full_path.len() >= 3 && full_path[full_path.len() - 3..] == *"ui/")
        || (parts.len() >= 2 && parts[1] != "assets" && parts[1].contains(".") == false)
    {
        full_path = "ui/index.html".to_string();
        content_type = TEXT_HTML;
    }

    // Build response based on wether the requested asset is found or not

    match Asset::get(&full_path[..]) {
        Some(ast) => match ast {
            Cow::Owned(data) => Box::new(future::ok(
                Response::builder()
                    .header("Content-Type", content_type)
                    .body(Body::from(data))
                    .unwrap(),
            )),
            Cow::Borrowed(data) => Box::new(future::ok(
                Response::builder()
                    .header("Content-Type", content_type)
                    .body(Body::from(data))
                    .unwrap(),
            )),
        },
        None => Box::new(future::ok(
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body("Not Found".into())
                .unwrap(),
        )),
    }
}

#[cfg(test)]
mod http_tests {
    use crate::config::{Config, LogAuth, Server, Token};

    use super::*;

    static VALID_TOKEN: &str = "TOKEN1TOKEN1TOKEN1TOKEN1TOKEN1TOKEN1TOKEN1TOKEN1";

    // Generates a Config object with only one auth item for one log
    fn get_auth_config_for(token: String, log_name: String) -> Config {
        let mut log_auth_map: HashMap<String, LogAuth> = HashMap::new();
        log_auth_map.insert(
            log_name.clone(),
            LogAuth {
                log_name: log_name,
                api: Vec::new(),
                expire: "".to_string(),
                status: "".to_string(),
            },
        );

        let mut auth = HashMap::new();
        auth.insert(token[0..16].to_string(), log_auth_map);

        let mut tokens = HashMap::new();
        tokens.insert(
            token[0..16].to_string(),
            Token {
                access_key: token[0..16].to_string(),
                secret_key: token[16..48].to_string(),
                description: None,
                is_admin: false,
                enabled: true,
            },
        );

        Config {
            server: Server {
                address: "".to_string(),
                metadata_endpoint: "".to_string(),
                metadata_bucket: "".to_string(),
                access_key: "".to_string(),
                secret_key: "".to_string(),
                pkcs12_cert: None,
                pkcs12_password: None,
            },
            datastore: HashMap::new(),
            tokens: tokens,
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
        let cfg = Arc::new(RwLock::new(cfg));
        // override the config
        let http_c = Http::new(cfg);

        let req: Request<Body>;

        let mut req2 = Request::builder();
        let mut req2 = req2.method(&case.method[..]);
        for (header, value) in case.headers {
            req2 = req2.header(&header[..], &value[..]);
        }
        req = req2.body(Body::from("test")).unwrap();

        let result = http_c.validate_token_from_header(&req);
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
            valid_token: VALID_TOKEN.to_string(),
            valid_log: "mylog".to_string(),
            method: "PUT".to_string(),
            headers: vec![("MINSQL-TOKEN".to_string(), VALID_TOKEN.to_string())],
            expected: HeaderToken::Token(VALID_TOKEN.to_string()),
            expected_token: Some(VALID_TOKEN.to_string()),
        })
    }

    #[test]
    fn missing_token_header() {
        run_test_validate_token_from_header(ValidTokenHeaderTest {
            valid_token: VALID_TOKEN.to_string(),
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
            valid_token: VALID_TOKEN.to_string(),
            valid_log: "mylog".to_string(),
            method: "PUT".to_string(),
            headers: vec![("MINSQL-TOKEN".to_string(), "TOKEN2".to_string())],
            expected: HeaderToken::InvalidToken,
            expected_token: Some("TOKEN2".to_string()),
        })
    }
}
