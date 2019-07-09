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
use crate::config::{Config, DataStore};
use crate::http::{return_400, return_404, ResponseFuture};
use crate::storage::{delete_object_metabucket, put_object_metabucket};
use futures::sink::Sink;
use futures::stream::Stream;
use futures::{future, Future};
use hyper::{header, Body, Chunk, Request, Response};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::unbounded_channel;

pub struct ApiDataStores {
    config: Arc<RwLock<Config>>,
}

impl SafeOutput for DataStore {
    fn safe(&mut self) {
        self.secret_key = "*********".to_string();
    }
}

impl ApiDataStores {
    pub fn new(cfg: Arc<RwLock<Config>>) -> ApiDataStores {
        ApiDataStores { config: cfg }
    }
}

impl ViewSet for ApiDataStores {
    fn list(&self, _req: Request<Body>) -> ResponseFuture {
        let cfg_read = self.config.read().unwrap();
        let mut datastores: Vec<DataStore> = Vec::new();
        for (_, ds) in &cfg_read.datastore {
            datastores.push(ds.clone());
        }
        let items = ListResponse {
            total: cfg_read.datastore.len(),
            next: None,
            previous: None,
            results: datastores,
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
                    let mut datastore: DataStore = match serde_json::from_str(&payload) {
                        Ok(v) => v,
                        Err(_) => {
                            return Ok(return_400("Could not parse request"));
                        }
                    };

                    // Validate Access/Secret
                    if datastore.access_key == "" || datastore.secret_key == "" {
                        return Ok(return_400("Access/Secret key cannot be empty."));
                    }
                    // Endpoint
                    if datastore.endpoint == "" {
                        return Ok(return_400("Endpoint cannot be empty."));
                    }
                    // Bucket
                    if datastore.bucket == "" {
                        return Ok(return_400("Bucket cannot be empty."));
                    }
                    let cfg_read = cfg.read().unwrap();

                    // Validate name
                    let mut datastore_name: String = "".to_string();
                    if let Some(ds_name) = &datastore.name {
                        if ds_name == "" {
                            return Ok(return_400("Datastore name cannot be empty."));
                        }
                        // validate datastore name uniqueness
                        if cfg_read.datastore.contains_key(ds_name) {
                            return Ok(return_400("Datastore name already in use"));
                        }
                        datastore_name = ds_name.clone();
                    }

                    // everything seems ok, create the datastore
                    let ds_serialized = serde_json::to_string(&datastore).unwrap();

                    let (tx, rx) = unbounded_channel();
                    let cfg = Arc::clone(&cfg);
                    tokio::spawn({
                        put_object_metabucket(
                            cfg,
                            format!("minsql/meta/datastores/{}", datastore_name),
                            ds_serialized,
                        )
                        .map_err(|_| ())
                        .and_then(move |_| {
                            datastore.safe();
                            let ds_serialized = serde_json::to_string(&datastore).unwrap();

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
        let mut datastore = match cfg_read.datastore.get(pk) {
            Some(ds) => ds.clone(),
            None => {
                return Box::new(future::ok(return_404()));
            }
        };
        datastore.safe();
        self.build_response(datastore)
    }

    fn update(&self, req: Request<Body>, pk: &str) -> ResponseFuture {
        let read_cfg = self.config.read().unwrap();
        if read_cfg.datastore.contains_key(pk) == false {
            return Box::new(future::ok(return_404()));
        }
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
                    let mut datastore: DataStore = match serde_json::from_str(&payload) {
                        Ok(v) => v,
                        Err(_) => {
                            return Ok(return_400("Could not parse request"));
                        }
                    };

                    // Validate Access/Secret
                    if datastore.access_key == "" || datastore.secret_key == "" {
                        return Ok(return_400("Access/Secret key cannot be empty."));
                    }
                    // Endpoint
                    if datastore.endpoint == "" {
                        return Ok(return_400("Endpoint cannot be empty."));
                    }
                    // Bucket
                    if datastore.bucket == "" {
                        return Ok(return_400("Bucket cannot be empty."));
                    }

                    // Validate name
                    let mut datastore_name: String = "".to_string();
                    if let Some(ds_name) = &datastore.name {
                        if ds_name == "" {
                            return Ok(return_400("Datastore name cannot be empty."));
                        }
                        datastore_name = ds_name.clone();
                    }
                    // if ds name changed, delete previous file
                    if datastore_name != pk {
                        let cfg = Arc::clone(&cfg);
                        tokio::spawn({
                            delete_object_metabucket(cfg, format!("minsql/meta/datastores/{}", pk))
                                .map(|_| ())
                                .map_err(|_| ())
                        });
                    }

                    // everything seems ok, write to datastore
                    let ds_serialized = serde_json::to_string(&datastore).unwrap();

                    let (tx, rx) = unbounded_channel();
                    let cfg = Arc::clone(&cfg);
                    tokio::spawn({
                        put_object_metabucket(
                            cfg,
                            format!("minsql/meta/datastores/{}", datastore_name),
                            ds_serialized.clone(),
                        )
                        .map_err(|_| {})
                        .and_then(move |_| {
                            //remove sensitive data
                            datastore.safe();
                            let ds_serialized = serde_json::to_string(&datastore).unwrap();
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
        let mut datastore = match read_cfg.datastore.get(pk) {
            Some(v) => v.clone(),
            None => {
                return Box::new(future::ok(return_404()));
            }
        };

        let ds_name = match &datastore.name {
            Some(v) => v.clone(),
            None => "".to_string(),
        };

        let (tx, rx) = unbounded_channel();
        let cfg = Arc::clone(&self.config);
        tokio::spawn({
            delete_object_metabucket(cfg, format!("minsql/meta/datastores/{}", ds_name))
                .map_err(|_| {})
                .and_then(move |_| {
                    //remove sensitive data
                    datastore.safe();
                    let ds_serialized = serde_json::to_string(&datastore).unwrap();
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
