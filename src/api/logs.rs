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
use std::sync::{Arc, RwLock};

use futures::sink::Sink;
use futures::stream::Stream;
use futures::{future, Future};
use hyper::{header, Body, Chunk, Request, Response};
use tokio::sync::mpsc::unbounded_channel;

use crate::api::{ListResponse, SafeOutput, ViewSet};
use crate::config::{Config, Log};
use crate::http::{return_400, return_404, ResponseFuture};
use crate::storage::{delete_object_metabucket, put_object_metabucket};

pub struct ApiLogs {
    config: Arc<RwLock<Config>>,
}

impl SafeOutput for Log {
    // Log has nothing to hide
    fn safe(&mut self) {}
}

impl ApiLogs {
    pub fn new(cfg: Arc<RwLock<Config>>) -> ApiLogs {
        ApiLogs { config: cfg }
    }
}

impl ViewSet for ApiLogs {
    /// Lists all logs
    fn list(&self, _req: Request<Body>) -> ResponseFuture {
        let cfg_read = self.config.read().unwrap();
        let mut logs: Vec<Log> = Vec::new();
        for (_, ds) in &cfg_read.log {
            logs.push(ds.clone());
        }
        let items = ListResponse {
            total: cfg_read.log.len(),
            next: None,
            previous: None,
            results: logs,
        };
        Box::new(self.build_response(items))
    }

    fn create(&self, req: Request<Body>) -> ResponseFuture {
        let cfg = Arc::clone(&self.config);
        Box::new(
            req.into_body()
                .concat2()
                .from_err()
                .and_then(move |entire_body| {
                    let payload: String = match String::from_utf8(entire_body.to_vec()) {
                        Ok(str) => str,
                        Err(_) => {
                            return Ok(return_400("Could not understand request"));
                        }
                    };
                    let mut log: Log = match serde_json::from_str(&payload) {
                        Ok(v) => v,
                        Err(_) => {
                            return Ok(return_400("Could not parse request"));
                        }
                    };

                    // Validate Commit Window
                    if log.commit_window == "" {
                        return Ok(return_400("Commit window key cannot be empty."));
                    }
                    if !log.commit_window.ends_with("s") && !log.commit_window.ends_with("m") {
                        return Ok(return_400("Commit window must be specified in either seconds `5s` or minutes `1m`"));
                    }

                    // if the commit window parses to 0 and the value is not 0, 0s or 0m, it's an invalid window
                    let parsed_window = Config::commit_window_to_seconds(&log.commit_window);
                    if log.commit_window != "0" 
                        && log.commit_window != "0s" 
                        && log.commit_window != "0m" 
                        && parsed_window == 0 {
                        return Ok(return_400("Commit window is invalid"));
                    }

                    let cfg_read = cfg.read().unwrap();
                    // validate the datastores
                    for ds_name in &log.datastores {
                        if cfg_read.datastore.contains_key(ds_name) == false {
                            return Ok(return_400(&format!("{} is an invalid datastore name", &ds_name)));
                        }
                    }

                    // Validate name

                    let mut log_name: String = "".to_string();
                    if let Some(lg_name) = &log.name {
                        if lg_name == "" {
                            return Ok(return_400("Log name cannot be empty."));
                        }
                        // validate datastore name uniqueness
                        if cfg_read.log.contains_key(lg_name) {
                            return Ok(return_400("Log name already in use"));
                        }
                        log_name = lg_name.clone();
                    }

                    // everything seems ok, create the datastore
                    let ds_serialized = serde_json::to_string(&log).unwrap();

                    let (tx, rx) = unbounded_channel();
                    let cfg = Arc::clone(&cfg);
                    tokio::spawn({
                        put_object_metabucket(
                            cfg,
                            format!("minsql/meta/logs/{}", log_name),
                            ds_serialized,
                        )
                            .map_err(|_| ())
                            .and_then(move |_| {
                                log.safe();
                                let ds_serialized = serde_json::to_string(&log).unwrap();

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

    fn retrieve(&self, _req: Request<Body>, pk: &str) -> ResponseFuture {
        let cfg_read = self.config.read().unwrap();
        let mut log = match cfg_read.log.get(pk) {
            Some(ds) => ds.clone(),
            None => {
                return Box::new(future::ok(return_404()));
            }
        };
        log.safe();
        self.build_response(log)
    }

    fn update(&self, req: Request<Body>, pk: &str) -> ResponseFuture {
        let pk = pk.to_string();

        let cfg = Arc::clone(&self.config);
        Box::new(
            req.into_body()
                .concat2()
                .from_err()
                .and_then(move |entire_body| {
                    let payload: String = match String::from_utf8(entire_body.to_vec()) {
                        Ok(str) => str,
                        Err(_) => {
                            return Ok(return_400("Could not understand request"));
                        }
                    };
                    let read_cfg = cfg.read().unwrap();
                    let mut current_log = match read_cfg.log.get(&pk) {
                        Some(v) => v.clone(),
                        None => {
                            return Ok(return_404());
                        }
                    };

                    let log: serde_json::Value = match serde_json::from_str(&payload) {
                        Ok(v) => v,
                        Err(_) => {
                            return Ok(return_400("Could not parse request"));
                        }
                    };

                    // Commit Window
                    if let Some(serde_json::Value::String(commit_window)) = log.get("commit_window") {
                        // Validate Commit Window
                        if commit_window == "" {
                            return Ok(return_400("Commit window key cannot be empty."));
                        }
                        if !commit_window.ends_with("s") && !commit_window.ends_with("m") {
                            return Ok(return_400("Commit window must be specified in either seconds `5s` or minutes `1m`"));
                        }
                        // if the commit window parses to 0 and the value is not 0, 0s or 0m, it's an invalid window
                        let parsed_window = Config::commit_window_to_seconds(&commit_window);
                        if commit_window != "0"
                            && commit_window != "0s"
                            && commit_window != "0m"
                            && parsed_window == 0 {
                            return Ok(return_400("Commit window is invalid"));
                        }
                        current_log.commit_window = commit_window.clone();
                    }

                    let cfg_read = cfg.read().unwrap();
                    // validate the datastores
                    if let Some(serde_json::Value::Array(datastores_value)) = log.get("datastores") {
                        for ds_name_value in datastores_value {
                            if let serde_json::Value::String(ds_name) = ds_name_value {
                                if cfg_read.datastore.contains_key(ds_name) == false {
                                    return Ok(return_400(&format!("{} is an invalid datastore name", &ds_name)));
                                }
                            }
                        }
                    }

                    // Validate name
                    let mut log_name: Option<String> = None;
                    if let Some(serde_json::Value::String(name)) = log.get("name") {
                        if name == "" {
                            return Ok(return_400("Log name cannot be empty."));
                        }
                        current_log.name = Some(name.clone());
                        log_name = Some(name.clone());
                    }

                    // if log name changed, delete previous file
                    if let Some(ds_name) = log_name {
                        if ds_name != pk {
                            let cfg = Arc::clone(&cfg);
                            tokio::spawn({
                                delete_object_metabucket(
                                    cfg,
                                    format!("minsql/meta/logs/{}", pk),
                                )
                                .map(|_| ())
                                .map_err(|_| ())
                            });
                        }
                    }

                    // everything seems ok, write to datastore
                    let ds_serialized = serde_json::to_string(&current_log).unwrap();

                    let (tx, rx) = unbounded_channel();
                    let cfg = Arc::clone(&cfg);
                    tokio::spawn({
                        put_object_metabucket(
                            cfg,
                            format!("minsql/meta/logs/{}", pk),
                            ds_serialized.clone(),
                        )
                        .map_err(|_| {})
                        .and_then(move |_| {
                            //remove sensitive data
                            current_log.safe();
                            let ds_serialized = serde_json::to_string(&current_log).unwrap();
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

    fn delete(&self, _req: Request<Body>, pk: &str) -> ResponseFuture {
        let read_cfg = self.config.read().unwrap();
        let mut log = match read_cfg.log.get(pk) {
            Some(v) => v.clone(),
            None => {
                return Box::new(future::ok(return_404()));
            }
        };

        let log_name = match &log.name {
            Some(v) => v.clone(),
            None => "".to_string(),
        };

        let (tx, rx) = unbounded_channel();
        let cfg = Arc::clone(&self.config);
        tokio::spawn({
            delete_object_metabucket(cfg, format!("minsql/meta/logs/{}", log_name))
                .map_err(|_| {})
                .and_then(move |_| {
                    //remove sensitive data
                    log.safe();
                    let ds_serialized = serde_json::to_string(&log).unwrap();
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
