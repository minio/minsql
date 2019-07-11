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
use crate::api::{ListResponse, SafeOutput, ViewSet};
use crate::config::{Config, LogAuth};
use crate::http::{return_400, return_404, ResponseFuture};
use crate::storage::{delete_object_metabucket, put_object_metabucket};
use futures::sink::Sink;
use futures::stream::Stream;
use futures::{future, Future};
use hyper::{header, Body, Chunk, Method, Request, Response};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::unbounded_channel;

pub struct ApiAuth {
    config: Arc<RwLock<Config>>,
}

impl SafeOutput for LogAuth {
    // No sensitive data on `LogAuth`
    fn safe(&mut self) {}
}

impl ApiAuth {
    pub fn new(cfg: Arc<RwLock<Config>>) -> ApiAuth {
        ApiAuth { config: cfg }
    }

    fn list(&self, _req: Request<Body>, token_access_key: &str) -> ResponseFuture {
        let cfg_read = self.config.read().unwrap();
        if cfg_read.tokens.contains_key(token_access_key) == false {
            return Box::new(future::ok(return_404()));
        }

        let mut auth: Vec<LogAuth> = Vec::new();
        let mut total: usize = 0;
        if let Some(log_map) = cfg_read.auth.get(token_access_key) {
            total = log_map.len();
            for (_, log_auth) in log_map {
                auth.push(log_auth.clone());
            }
        }

        let items = ListResponse {
            total: total,
            next: None,
            previous: None,
            results: auth,
        };
        Box::new(self.build_response(items))
    }

    fn create(&self, req: Request<Body>, token_access_key: &str) -> ResponseFuture {
        let cfg = Arc::clone(&self.config);
        let token_access_key_clone = token_access_key.to_string();
        Box::new(
            req.into_body()
                .concat2()
                .from_err()
                .and_then(move |entire_body| {
                    let cfg_read = cfg.read().unwrap();
                    // validate token
                    if cfg_read.tokens.contains_key(&token_access_key_clone) == false {
                        return Ok(return_404());
                    }
                    let payload: String = match String::from_utf8(entire_body.to_vec()) {
                        Ok(str) => str,
                        Err(_) => {
                            return Ok(return_400("Could not understand request"));
                        }
                    };
                    //default token
                    let mut new_log_auth: LogAuth = LogAuth {
                        log_name: "".to_string(),
                        api: vec![],
                        expire: "".to_string(),
                        status: "".to_string(),
                    };

                    let log_auth: serde_json::Value = match serde_json::from_str(&payload) {
                        Ok(v) => v,
                        Err(_) => {
                            return Ok(return_400("Could not parse request"));
                        }
                    };

                    // Validate log name
                    if let Some(serde_json::Value::String(log_name)) = log_auth.get("log_name") {
                        if log_name == "" {
                            return Ok(return_400("Log name cannot be empty"));
                        }
                        // validate log_name uniqueness
                        if let Some(log_map) = cfg_read.auth.get(&token_access_key_clone) {
                            if log_map.contains_key(log_name) {
                                return Ok(return_400(&format!(
                                    "Auth already given for log {} in token {}",
                                    &log_name, &token_access_key_clone,
                                )));
                            }
                        }
                        new_log_auth.log_name = log_name.clone();
                    }

                    if let Some(serde_json::Value::Array(api_value)) = log_auth.get("api") {
                        let mut apis: Vec<String> = Vec::new();
                        for v in api_value {
                            if let serde_json::Value::String(api) = v {
                                // validate the API
                                if api != "search" && api != "store" {
                                    return Ok(return_400(&format!(
                                        "Unknown API {} provided",
                                        api
                                    )));
                                }
                                apis.push(api.clone());
                            }
                        }
                        new_log_auth.api = apis;
                    }

                    if let Some(serde_json::Value::String(expire)) = log_auth.get("expire") {
                        new_log_auth.expire = expire.clone();
                    }

                    if let Some(serde_json::Value::String(status)) = log_auth.get("status") {
                        new_log_auth.status = status.clone();
                    }

                    // everything seems ok, create the token
                    let token_serialized = serde_json::to_string(&new_log_auth).unwrap();

                    let (tx, rx) = unbounded_channel();
                    let cfg = Arc::clone(&cfg);
                    tokio::spawn({
                        put_object_metabucket(
                            cfg,
                            format!(
                                "minsql/meta/auth/{}/{}",
                                token_access_key_clone, &new_log_auth.log_name
                            ),
                            token_serialized,
                        )
                        .map_err(|_| ())
                        .and_then(move |_| {
                            new_log_auth.safe();
                            let ds_serialized = serde_json::to_string(&new_log_auth).unwrap();

                            tx.send(ds_serialized).map_err(|_| ())
                        })
                        .map(|_| ())
                        .map_err(|_| ())
                    });

                    let body_str = rx.map_err(|e| e).map(|x| Chunk::from(x));
                    let mut response = Response::builder();
                    response.header(header::CONTENT_TYPE, "application/json");

                    Ok(response.body(Body::wrap_stream(body_str)).unwrap())
                }),
        )
    }

    fn retrieve(&self, _req: Request<Body>, token_access_key: &str, pk: &str) -> ResponseFuture {
        let cfg_read = self.config.read().unwrap();
        if cfg_read.tokens.contains_key(token_access_key) == false {
            return Box::new(future::ok(return_404()));
        }
        let mut auth = match cfg_read.auth.get(token_access_key) {
            Some(token_logs) => match token_logs.get(pk) {
                Some(log_auth) => log_auth.clone(),
                None => {
                    return Box::new(future::ok(return_404()));
                }
            },
            None => {
                return Box::new(future::ok(return_404()));
            }
        };
        auth.safe();
        self.build_response(auth)
    }

    fn update(&self, req: Request<Body>, token_access_key: &str, pk: &str) -> ResponseFuture {
        let pk = pk.to_string();
        let token_access_key_clone = token_access_key.to_string();
        let pk_clone = pk.to_string();
        let cfg = Arc::clone(&self.config);
        Box::new(
            req.into_body()
                .concat2()
                .from_err()
                .and_then(move |entire_body| {
                    let cfg_read = cfg.read().unwrap();
                    // validate token
                    if cfg_read.tokens.contains_key(&token_access_key_clone) == false {
                        return Ok(return_404());
                    }
                    let payload: String = match String::from_utf8(entire_body.to_vec()) {
                        Ok(str) => str,
                        Err(_) => {
                            return Ok(return_400("Could not understand request"));
                        }
                    };

                    let mut current_log_auth = match cfg_read.auth.get(&token_access_key_clone) {
                        Some(token_logs) => match token_logs.get(&pk_clone) {
                            Some(log_auth) => log_auth.clone(),
                            None => {
                                return Ok(return_404());
                            }
                        },
                        None => {
                            return Ok(return_404());
                        }
                    };

                    let log_auth: serde_json::Value = match serde_json::from_str(&payload) {
                        Ok(v) => v,
                        Err(_) => {
                            return Ok(return_400("Could not parse request"));
                        }
                    };

                    // Validate log name
                    if let Some(serde_json::Value::String(log_name)) = log_auth.get("log_name") {
                        if log_name == "" {
                            return Ok(return_400("Log name cannot be empty"));
                        }
                        // validate log_name uniqueness
                        if let Some(log_map) = cfg_read.auth.get(&token_access_key_clone) {
                            if log_map.contains_key(log_name) {
                                return Ok(return_400(&format!(
                                    "Auth already given for log {} in token {}",
                                    &log_name, &token_access_key_clone,
                                )));
                            }
                        }
                        current_log_auth.log_name = log_name.clone();
                    }

                    if let Some(serde_json::Value::Array(api_value)) = log_auth.get("api") {
                        let mut apis: Vec<String> = Vec::new();
                        for v in api_value {
                            if let serde_json::Value::String(api) = v {
                                // validate the API
                                if api != "search" && api != "store" {
                                    return Ok(return_400(&format!(
                                        "Unknown API {} provided",
                                        api
                                    )));
                                }
                                apis.push(api.clone());
                            }
                        }
                        current_log_auth.api = apis;
                    }

                    if let Some(serde_json::Value::String(expire)) = log_auth.get("expire") {
                        current_log_auth.expire = expire.clone();
                    }

                    if let Some(serde_json::Value::String(status)) = log_auth.get("status") {
                        current_log_auth.status = status.clone();
                    }

                    // everything seems ok, write to token
                    let ds_serialized = serde_json::to_string(&current_log_auth).unwrap();

                    let (tx, rx) = unbounded_channel();
                    let cfg = Arc::clone(&cfg);
                    tokio::spawn({
                        put_object_metabucket(
                            cfg,
                            format!("minsql/meta/auth/{}/{}", token_access_key_clone, pk_clone),
                            ds_serialized.clone(),
                        )
                        .map_err(|_| {})
                        .and_then(move |_| {
                            //remove sensitive data
                            current_log_auth.safe();
                            let ds_serialized = serde_json::to_string(&current_log_auth).unwrap();
                            tx.send(ds_serialized).map_err(|_| ())
                        })
                        .map(|_| ())
                        .map_err(|_| ())
                    });

                    let body_str = rx.map_err(|e| e).map(|x| Chunk::from(x));
                    let mut response = Response::builder();
                    response.header(header::CONTENT_TYPE, "application/json");

                    Ok(response.body(Body::wrap_stream(body_str)).unwrap())
                }),
        )
    }

    fn delete(&self, _req: Request<Body>, token_access_key: &str, pk: &str) -> ResponseFuture {
        let cfg_read = self.config.read().unwrap();
        if cfg_read.tokens.contains_key(token_access_key) == false {
            return Box::new(future::ok(return_404()));
        }
        let mut log_auth = match cfg_read.auth.get(token_access_key) {
            Some(token_logs) => match token_logs.get(pk) {
                Some(log_auth) => log_auth.clone(),
                None => {
                    return Box::new(future::ok(return_404()));
                }
            },
            None => {
                return Box::new(future::ok(return_404()));
            }
        };

        let (tx, rx) = unbounded_channel();
        let cfg = Arc::clone(&self.config);
        tokio::spawn({
            delete_object_metabucket(cfg, format!("minsql/meta/auth/{}/{}", token_access_key, pk))
                .map_err(|_| {})
                .and_then(move |_| {
                    //remove sensitive data
                    log_auth.safe();
                    let ds_serialized = serde_json::to_string(&log_auth).unwrap();
                    tx.send(ds_serialized).map_err(|_| ())
                })
                .map(|_| ())
                .map_err(|_| ())
        });

        let body_str = rx.map_err(|e| e).map(|x| Chunk::from(x));
        let mut response = Response::builder();
        response.header(header::CONTENT_TYPE, "application/json");

        Box::new(future::ok(
            response.body(Body::wrap_stream(body_str)).unwrap(),
        ))
    }
}

impl ViewSet for ApiAuth {
    // No OP for regular access
    fn list(&self, _req: Request<Body>) -> ResponseFuture {
        return Box::new(future::ok(return_404()));
    }

    fn create(&self, _req: Request<Body>) -> ResponseFuture {
        return Box::new(future::ok(return_404()));
    }

    fn retrieve(&self, _req: Request<Body>, _pk: &str) -> ResponseFuture {
        return Box::new(future::ok(return_404()));
    }

    fn update(&self, _req: Request<Body>, _pk: &str) -> ResponseFuture {
        return Box::new(future::ok(return_404()));
    }

    fn delete(&self, _req: Request<Body>, _pk: &str) -> ResponseFuture {
        return Box::new(future::ok(return_404()));
    }

    /// route request.
    fn route(&self, req: Request<Body>, path_parts: Vec<&str>) -> ResponseFuture {
        match (req.method(), path_parts.get(2), path_parts.get(3)) {
            // delegate to proper action
            (&Method::GET, Some(token_access_key), None) => self.list(req, token_access_key),
            (&Method::POST, Some(token_access_key), None) => self.create(req, token_access_key),
            (&Method::GET, Some(token_access_key), Some(pk)) => {
                self.retrieve(req, token_access_key, pk)
            }
            (&Method::PUT, Some(token_access_key), Some(pk)) => {
                self.update(req, token_access_key, pk)
            }
            (&Method::DELETE, Some(token_access_key), Some(pk)) => {
                self.delete(req, token_access_key, pk)
            }
            _ => Box::new(future::ok(return_404())),
        }
    }
}
