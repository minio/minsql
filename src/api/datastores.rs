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

use futures::future::Either;
use futures::stream::Stream;
use futures::{future, Future};
use hyper::{header, Body, Chunk, Request, Response};

use crate::api::{SafeOutput, ViewSet};
use crate::config::{Config, DataStore};
use crate::http::{return_400, return_404, return_500, ResponseFuture};
use crate::storage::{delete_object_metabucket, put_object_metabucket};

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

    // Parses the datastore from the create body; returns error response in
    // case it is not valid.
    fn parse_create_body(
        entire_body: Vec<u8>,
        cfg: Arc<RwLock<Config>>,
    ) -> Result<DataStore, Response<Body>> {
        let payload: String = match String::from_utf8(entire_body.to_vec()) {
            Ok(str) => str,
            Err(_) => {
                return Err(return_400("Could not understand request"));
            }
        };
        let datastore: DataStore = match serde_json::from_str(&payload) {
            Ok(v) => v,
            Err(e) => {
                println!("{:?}", e);
                return Err(return_400("Could not parse request"));
            }
        };

        // Validate Access/Secret
        if datastore.access_key == "" || datastore.secret_key == "" {
            return Err(return_400("Access/Secret key cannot be empty."));
        }
        // Endpoint
        if datastore.endpoint == "" {
            return Err(return_400("Endpoint cannot be empty."));
        }
        // Bucket
        if datastore.bucket == "" {
            return Err(return_400("Bucket cannot be empty."));
        }
        let cfg_read = cfg.read().unwrap();

        // Validate name
        if let Some(ds_name) = &datastore.name {
            if ds_name == "" {
                return Err(return_400("Datastore name cannot be empty."));
            }
            // validate datastore name uniqueness
            if cfg_read.datastore.contains_key(ds_name) {
                return Err(return_400("Datastore name already in use"));
            }
        }
        Ok(datastore)
    }

    fn parse_update_body(
        entire_body: Vec<u8>,
        cfg: Arc<RwLock<Config>>,
        pk: &String,
    ) -> Result<DataStore, Response<Body>> {
        let payload: String = match String::from_utf8(entire_body.to_vec()) {
            Ok(str) => str,
            Err(_) => {
                return Err(return_400("Could not understand request"));
            }
        };
        let read_cfg = cfg.read().unwrap();
        let mut current_datastore = match read_cfg.datastore.get(pk) {
            Some(v) => v.clone(),
            None => {
                return Err(return_404());
            }
        };

        let datastore: HashMap<String, String> = match serde_json::from_str(&payload) {
            Ok(v) => v,
            Err(_) => {
                return Err(return_400("Could not parse request"));
            }
        };

        // Validate Access/Secret
        if let Some(access_key) = datastore.get("access_key") {
            if access_key == "" {
                return Err(return_400("Access key cannot be empty."));
            }
            current_datastore.access_key = access_key.clone();
        }
        if let Some(secret_key) = datastore.get("secret_key") {
            if secret_key == "" {
                return Err(return_400("Secret key cannot be empty."));
            }
            current_datastore.secret_key = secret_key.clone();
        }
        // Endpoint
        if let Some(endpoint) = datastore.get("endpoint") {
            if endpoint == "" {
                return Err(return_400("Endpoint cannot be empty."));
            }
            current_datastore.endpoint = endpoint.clone();
        }

        // Bucket
        if let Some(bucket) = datastore.get("bucket") {
            if bucket == "" {
                return Err(return_400("Bucket cannot be empty."));
            }
            current_datastore.bucket = bucket.clone();
        }

        // Prefix
        if let Some(prefix) = datastore.get("prefix") {
            current_datastore.prefix = prefix.clone();
        }

        // Validate name
        let mut datastore_name: Option<String> = None;
        if let Some(name) = datastore.get("name") {
            if name == "" {
                return Err(return_400("Datastore name cannot be empty."));
            }
            current_datastore.name = Some(name.clone());
            datastore_name = Some(name.clone());
        }

        // if ds name changed, delete previous file
        if let Some(ds_name) = datastore_name {
            if ds_name != *pk {
                let cfg = Arc::clone(&cfg);
                tokio::spawn({
                    delete_object_metabucket(cfg, format!("minsql/meta/datastores/{}", pk))
                        .map(|_| ())
                        .map_err(|_| ())
                });
            }
        }
        Ok(current_datastore)
    }
}

impl ViewSet for ApiDataStores {
    fn list(&self, req: Request<Body>) -> ResponseFuture {
        let cfg_read = self.config.read().unwrap();
        let mut datastores: Vec<DataStore> = Vec::new();
        for (_, ds) in &cfg_read.datastore {
            datastores.push(ds.clone());
        }
        // sort items
        datastores.sort_by(|a, b| a.name.cmp(&b.name));
        // paginate
        let items = self.paginate(req, datastores);
        Box::new(self.build_response(items))
    }

    fn create(&self, req: Request<Body>) -> ResponseFuture {
        let cfg = Arc::clone(&self.config);
        let cfg2 = Arc::clone(&self.config);
        Box::new(
            req.into_body()
                .concat2()
                .from_err()
                .and_then(move |entire_body| {
                    match ApiDataStores::parse_create_body(entire_body.to_vec(), cfg) {
                        Ok(mut datastore) => {
                            // everything seems ok, create the datastore
                            let ds_serialized = serde_json::to_string(&datastore).unwrap();
                            let datastore_name = datastore.name.clone().unwrap();

                            let cfg = Arc::clone(&cfg2);
                            let res = put_object_metabucket(
                                cfg,
                                format!("minsql/meta/datastores/{}", datastore_name),
                                ds_serialized,
                            )
                            .map_err(|_| ())
                            .then(move |v| match v {
                                Ok(_) => {
                                    datastore.safe();
                                    let ds_serialized = serde_json::to_string(&datastore).unwrap();

                                    let body = Body::from(Chunk::from(ds_serialized));
                                    let mut response = Response::builder();
                                    response.header(header::CONTENT_TYPE, "application/json");

                                    future::ok(response.body(body).unwrap())
                                }
                                Err(_) => future::ok(return_500("error saving datastore")),
                            });
                            Either::A(res)
                        }
                        Err(e) => Either::B(future::ok(e)),
                    }
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
        let pk = pk.to_string();

        let cfg = Arc::clone(&self.config);
        let cfg2 = Arc::clone(&self.config);
        Box::new(
            req.into_body()
                .concat2()
                .from_err()
                .and_then(move |entire_body| {
                    match ApiDataStores::parse_update_body(entire_body.to_vec(), cfg, &pk) {
                        Ok(mut current_datastore) => {
                            // everything seems ok, write to datastore
                            let ds_serialized = serde_json::to_string(&current_datastore).unwrap();

                            let res = put_object_metabucket(
                                cfg2,
                                format!("minsql/meta/datastores/{}", pk),
                                ds_serialized.clone(),
                            )
                            .map_err(|_| {})
                            .then(move |v| match v {
                                Ok(_) => {
                                    //remove sensitive data
                                    current_datastore.safe();
                                    let ds_serialized =
                                        serde_json::to_string(&current_datastore).unwrap();
                                    let body = Body::from(Chunk::from(ds_serialized));
                                    let mut response = Response::builder();
                                    response.header(header::CONTENT_TYPE, "application/json");

                                    future::ok(response.body(body).unwrap())
                                }
                                Err(_) => future::ok(return_500("error saving datastore")),
                            });

                            Either::A(res)
                        }
                        Err(e) => Either::B(future::ok(e)),
                    }
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

        let cfg = Arc::clone(&self.config);
        Box::new(
            delete_object_metabucket(cfg, format!("minsql/meta/datastores/{}", ds_name))
                .map_err(|_| {})
                .then(move |v| match v {
                    Ok(_) => {
                        //remove sensitive data
                        datastore.safe();
                        let ds_serialized = serde_json::to_string(&datastore).unwrap();
                        let body = Body::from(Chunk::from(ds_serialized));
                        let mut response = Response::builder();
                        response.header(header::CONTENT_TYPE, "application/json");

                        future::ok(response.body(body).unwrap())
                    }
                    Err(_) => future::ok(return_500("error deleting datastore from storage")),
                }),
        )
    }
}
