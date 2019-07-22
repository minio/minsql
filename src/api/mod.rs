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
use std::sync::{Arc, RwLock};

use futures::future;
use hyper::{header, Body, Method, Request, Response};
use serde::Serialize;
use serde_derive::Serialize;

use crate::api::auth::ApiAuth;
use crate::api::datastores::ApiDataStores;
use crate::api::logs::ApiLogs;
use crate::api::tokens::ApiTokens;
use crate::config::Config;
use crate::http::{return_401, return_404, HeaderToken, Http, ResponseFuture};

pub mod auth;
pub mod datastores;
pub mod logs;
pub mod tokens;

pub struct Api {
    config: Arc<RwLock<Config>>,
}

impl Api {
    pub fn new(cfg: Arc<RwLock<Config>>) -> Api {
        Api { config: cfg }
    }

    /// Routes a request to the proper module, or returns a 404 if nothing is matched.
    pub fn router(&self, req: Request<Body>, path_parts: Vec<&str>) -> ResponseFuture {
        // validate access token on headers
        let http_c = Http::new(Arc::clone(&self.config));
        match http_c.validate_token_from_header(&req) {
            HeaderToken::Token(token) => {
                //validate the token is admin
                let read_cfg = self.config.read().unwrap();
                match read_cfg.tokens.get(&token[0..16]) {
                    Some(tk) => {
                        if tk.is_admin == false {
                            return Box::new(future::ok(return_401()));
                        }
                    }
                    None => {
                        return Box::new(future::ok(return_401()));
                    }
                }
            }
            HeaderToken::InvalidToken => {
                return Box::new(future::ok(return_401()));
            }
            HeaderToken::NoToken => {
                return Box::new(future::ok(return_401()));
            }
        }
        match path_parts.get(1) {
            // delegate to proper module
            Some(&"auth") => {
                let auths = ApiAuth::new(Arc::clone(&self.config));
                auths.route(req, path_parts)
            }
            Some(&"datastores") => {
                let datastores = ApiDataStores::new(Arc::clone(&self.config));
                datastores.route(req, path_parts)
            }
            Some(&"logs") => {
                let logs = ApiLogs::new(Arc::clone(&self.config));
                logs.route(req, path_parts)
            }
            Some(&"tokens") => {
                let auths = ApiTokens::new(Arc::clone(&self.config));
                auths.route(req, path_parts)
            }
            _ => Box::new(future::ok(return_404())),
        }
    }
}

/// Standard REST behavior.
pub trait ViewSet {
    // Fulfills a GET operation, which should list items
    fn list(&self, req: Request<Body>) -> ResponseFuture;
    // POST: Creates an object upon POST
    fn create(&self, req: Request<Body>) -> ResponseFuture;
    // GET: Retrieves an object via primary key
    fn retrieve(&self, req: Request<Body>, pk: &str) -> ResponseFuture;
    // PUT: Updates an object via primary key,
    fn update(&self, req: Request<Body>, pk: &str) -> ResponseFuture;
    // DELETE: Removes an individual object
    fn delete(&self, req: Request<Body>, pk: &str) -> ResponseFuture;

    /// route request.
    fn route(&self, req: Request<Body>, path_parts: Vec<&str>) -> ResponseFuture {
        match (req.method(), path_parts.get(2)) {
            // delegate to proper action
            (&Method::GET, None) => self.list(req),
            (&Method::POST, None) => self.create(req),
            (&Method::GET, Some(pk)) => self.retrieve(req, pk),
            (&Method::PUT, Some(pk)) => self.update(req, pk),
            (&Method::DELETE, Some(pk)) => self.delete(req, pk),
            _ => Box::new(future::ok(return_404())),
        }
    }
    /// Builds a json response for an object `T` that is serializable.
    fn build_response<T>(&self, mut obj: T) -> ResponseFuture
    where
        T: Serialize,
        T: SafeOutput,
    {
        // Make sure output has no sensitive data
        obj.safe();
        // Serialize it to a JSON string.
        let output = serde_json::to_string(&obj).unwrap();
        // Construct body
        let body = Body::from(output);
        let mut response = Response::builder();
        response.header(header::CONTENT_TYPE, "application/json");
        Box::new(future::ok(response.body(body).unwrap()))
    }

    /// Takes a list of objects, the request and returns a sublist of items (aka page)
    fn paginate<T>(&self, request: Request<Body>, obj: Vec<T>) -> ListResponse<T>
    where
        T: Serialize,
        T: SafeOutput,
    {
        let query_params = self.parse_query_parameters(&request);

        let offset: usize = query_params
            .get("offset")
            .unwrap_or(&"0".to_string())
            .parse()
            .unwrap_or(0);

        let limit: usize = query_params
            .get("limit")
            .unwrap_or(&"10".to_string())
            .parse()
            .unwrap_or(10);

        ListResponse {
            total: obj.len(),
            next: None,
            previous: None,
            results: obj.into_iter().skip(offset).take(limit).collect(),
        }
    }

    /// Parses the query parameters from a uri on a request and returns a `HashMap<String,String>`
    /// with (key,value) note that repeated values overwrite over the previous value under the same
    /// key.
    fn parse_query_parameters(&self, req: &Request<Body>) -> HashMap<String, String> {
        let mut query_params: HashMap<String, String> = HashMap::new();
        if let Some(query) = req.uri().query() {
            let parse = url::form_urlencoded::parse(&query.as_ref());
            for (key, value) in parse {
                query_params.insert(key.to_string(), value.to_string());
            }
        }
        query_params
    }
}

/// Trait that mandates content be cleared of any sensitive information (secret_key, password, etc)
pub trait SafeOutput {
    /// Clears the struct of any sensitive data.
    fn safe(&mut self);
}

#[derive(Debug, Serialize)]
pub struct ListResponse<T>
where
    T: Serialize,
    T: SafeOutput,
{
    pub total: usize,
    pub next: Option<String>,
    pub previous: Option<String>,
    pub results: Vec<T>,
}

impl<T> SafeOutput for ListResponse<T>
where
    T: Serialize,
    T: SafeOutput,
{
    fn safe(&mut self) {
        for i in 0..self.results.len() {
            self.results[i].safe();
        }
    }
}
