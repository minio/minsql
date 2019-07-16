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

use futures::future::Either;
use futures::{future, Future, Stream};
use hyper::{header, Body, Chunk, Request, Response};

use crate::api::{ListResponse, SafeOutput, ViewSet};
use crate::config::{Config, Log};
use crate::http::{return_400, return_404, return_500, ResponseFuture};
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

    // Parses the log from the create body; returns error response in
    // case it is not valid.
    fn parse_create_body(
        entire_body: Vec<u8>,
        cfg: Arc<RwLock<Config>>,
    ) -> Result<Log, Response<Body>> {
        let payload = String::from_utf8(entire_body)
            .map_err(|_| return_400("Could not understand request"))?;

        let log: Log =
            serde_json::from_str(&payload).map_err(|_| return_400("Could not parse request"))?;

        // Validate Commit Window
        if log.commit_window == "" {
            return Err(return_400("Commit window key cannot be empty."));
        }
        if !log.commit_window.ends_with("s") && !log.commit_window.ends_with("m") {
            return Err(return_400(
                "Commit window must be specified in either seconds `5s` or minutes `1m`",
            ));
        }

        // if the commit window parses to 0 and the value is not 0, 0s or 0m, it's an invalid window
        let parsed_window = Config::commit_window_to_seconds(&log.commit_window);
        if parsed_window.is_none() {
            return Err(return_400("Commit window is invalid"));
        }

        let cfg_read = cfg.read().unwrap();
        // validate the datastores
        for ds_name in &log.datastores {
            if cfg_read.datastore.contains_key(ds_name) == false {
                return Err(return_400(&format!(
                    "{} is an invalid datastore name",
                    &ds_name
                )));
            }
        }

        // Validate name

        if let Some(lg_name) = &log.name {
            if lg_name == "" {
                return Err(return_400("Log name cannot be empty."));
            }
            // validate datastore name uniqueness
            if cfg_read.log.contains_key(lg_name) {
                return Err(return_400("Log name already in use"));
            }
        }

        Ok(log)
    }

    // Parses the log from the create body; returns error response in
    // case it is not valid.
    fn parse_update_body(
        entire_body: Vec<u8>,
        cfg: Arc<RwLock<Config>>,
        pk: String,
    ) -> Result<Log, Response<Body>> {
        let payload: String = String::from_utf8(entire_body.to_vec())
            .map_err(|_| return_400("Could not understand request"))?;
        let read_cfg = cfg.read().unwrap();
        let mut current_log = match read_cfg.log.get(&pk) {
            Some(v) => v.clone(),
            None => {
                return Err(return_404());
            }
        };

        let log: serde_json::Value =
            serde_json::from_str(&payload).map_err(|_| return_400("Could not parse request"))?;

        // Commit Window
        if let Some(serde_json::Value::String(commit_window)) = log.get("commit_window") {
            // Validate Commit Window
            if commit_window == "" {
                return Err(return_400("Commit window key cannot be empty."));
            }
            if !commit_window.ends_with("s") && !commit_window.ends_with("m") {
                return Err(return_400(
                    "Commit window must be specified in either seconds `5s` or minutes `1m`",
                ));
            }
            // if the commit window parses to 0 and the value is not 0, 0s or 0m, it's an invalid window
            let parsed_window = Config::commit_window_to_seconds(&commit_window);
            if parsed_window.is_none() {
                return Err(return_400("Commit window is invalid"));
            }
            current_log.commit_window = commit_window.clone();
        }

        let cfg_read = cfg.read().unwrap();
        // validate the datastores
        if let Some(serde_json::Value::Array(datastores_value)) = log.get("datastores") {
            let mut datastores: Vec<String> = Vec::new();
            for ds_name_value in datastores_value {
                if let serde_json::Value::String(ds_name) = ds_name_value {
                    if cfg_read.datastore.contains_key(ds_name) == false {
                        return Err(return_400(&format!(
                            "{} is an invalid datastore name",
                            &ds_name
                        )));
                    } else {
                        datastores.push(ds_name.clone());
                    }
                }
            }
            current_log.datastores = datastores;
        }

        // Validate name
        let mut log_name: Option<String> = None;
        if let Some(serde_json::Value::String(name)) = log.get("name") {
            if name == "" {
                return Err(return_400("Log name cannot be empty."));
            }
            current_log.name = Some(name.clone());
            log_name = Some(name.clone());
        }

        // if log name changed, delete previous file
        if let Some(ds_name) = log_name {
            if ds_name != pk {
                let cfg = Arc::clone(&cfg);
                tokio::spawn({
                    delete_object_metabucket(cfg, format!("minsql/meta/logs/{}", pk))
                        .map(|_| ())
                        .map_err(|_| ())
                });
            }
        }
        Ok(current_log)
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
                    match ApiLogs::parse_create_body(entire_body.to_vec(), cfg.clone()) {
                        Ok(mut log) => {
                            let ds_serialized = serde_json::to_string(&log).unwrap();
                            let log_name = log.clone().name.unwrap();

                            let res = put_object_metabucket(
                                cfg,
                                format!("minsql/meta/logs/{}", log_name),
                                ds_serialized,
                            )
                            .then(move |v| match v {
                                Ok(_) => {
                                    log.safe();
                                    future::ok(
                                        Response::builder()
                                            .header(header::CONTENT_TYPE, "application/json")
                                            .body(Body::from(serde_json::to_string(&log).unwrap()))
                                            .unwrap(),
                                    )
                                }
                                Err(e) => future::ok(return_500(&format!("I/O Err: {}", e))),
                            });
                            Either::A(res)
                        }
                        Err(err_resp) => Either::B(future::ok(err_resp)),
                    }
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
                    match ApiLogs::parse_update_body(entire_body.to_vec(), cfg.clone(), pk) {
                        Ok(mut log) => {
                            let ds_serialized = serde_json::to_string(&log).unwrap();
                            let log_name = log.clone().name.unwrap();

                            let res = put_object_metabucket(
                                cfg,
                                format!("minsql/meta/logs/{}", log_name),
                                ds_serialized,
                            )
                            .then(move |v| match v {
                                Ok(_) => {
                                    log.safe();
                                    future::ok(
                                        Response::builder()
                                            .header(header::CONTENT_TYPE, "application/json")
                                            .body(Body::from(serde_json::to_string(&log).unwrap()))
                                            .unwrap(),
                                    )
                                }
                                Err(e) => future::ok(return_500(&format!("I/O Err: {}", e))),
                            });
                            Either::A(res)
                        }
                        Err(err_resp) => Either::B(future::ok(err_resp)),
                    }
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

        Box::new(
            delete_object_metabucket(
                Arc::clone(&self.config),
                format!("minsql/meta/logs/{}", log_name),
            )
            .map_err(|_| {
                println!("Some error deleting");
                return_500("Error deleting")
            })
            .then(move |_| {
                //remove sensitive data
                log.safe();
                let ds_serialized = serde_json::to_string(&log).unwrap();
                let body = Body::from(Chunk::from(ds_serialized));
                let mut response = Response::builder();
                response.header(header::CONTENT_TYPE, "application/json");
                future::ok(response.body(body).unwrap())
            }),
        )
    }
}
