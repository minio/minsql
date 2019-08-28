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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::process;
use std::sync::{Arc, RwLock};

use futures::future::Future;
use futures::stream;
use futures::Stream;
use log::{error, info};
use minio_rs::minio;
use minio_rs::minio::Credentials;
use rusoto_s3::{GetObjectRequest, ListObjectsRequest, S3};

use crate::config::{Config, DataStore, Log, LogAuth, Token};
use crate::storage;

pub struct Meta {
    config: Arc<RwLock<Config>>,
}

impl Meta {
    pub fn new(cfg: Arc<RwLock<Config>>) -> Meta {
        Meta { config: cfg }
    }

    /// Scans the metabucket for configuration files and loads them into the shared state `Config`
    pub fn load_config_from_metabucket(&self) -> impl Future<Item = (), Error = ()> {
        // validate access to the metadata store
        let ds = ds_for_metabucket(Arc::clone(&self.config));
        match storage::can_reach_datastore(&ds) {
            Ok(true) => (),
            Ok(false) => {
                println!("Metabucket is not reachable");
                process::exit(0x0100);
            }
            Err(e) => match e {
                storage::StorageError::Operation(
                    storage::ReachableDatastoreError::NoSuchBucket(s),
                ) => {
                    println!("Metabucket doesn't exists: {:?}", s);
                    process::exit(0x0100);
                }
                _ => {
                    println!("Metabucket is not reachable");
                    process::exit(0x0100);
                }
            },
        }

        // Create s3 client
        let s3_client = storage::client_for_datastore(&ds);
        let s3_client = Arc::new(s3_client);

        let s3_client1 = Arc::clone(&s3_client);
        let s3_client2 = Arc::clone(&s3_client);

        let main_cfg = Arc::clone(&self.config);

        let bucket_name = ds.bucket.clone();
        let bucket_name2 = ds.bucket.clone();
        // get all the objects inside the meta folder
        let task = stream::unfold(Some("".to_string()), move |state| match state {
            None => None,
            Some(marker) => {
                let bucket_name = bucket_name.clone();
                Some(
                    s3_client1
                        .list_objects(ListObjectsRequest {
                            bucket: bucket_name,
                            prefix: Some("minsql/meta/".to_owned()),
                            marker: Some(marker),
                            ..Default::default()
                        })
                        .map(|list_objects| {
                            let objs = list_objects
                                .contents
                                .unwrap_or(vec![])
                                .into_iter()
                                .map(|x| x.key.unwrap())
                                // Avoid loading models
                                .filter(|file_key| file_key.contains("/models/") == false)
                                .collect();

                            (objs, list_objects.next_marker)
                        }),
                )
            }
        })
        .map(|x: Vec<String>| stream::iter_ok(x))
        .map_err(|_| ())
        .flatten()
        .map(move |file_key: String| {
            let file_key_clone = file_key.clone();
            let bucket_name3 = bucket_name2.clone();
            s3_client2
                .get_object(GetObjectRequest {
                    bucket: bucket_name3,
                    key: file_key,
                    ..Default::default()
                })
                .map_err(|e| {
                    error!("getting object: {:?}", e);
                    ()
                })
                .and_then(|object_output| {
                    // Deserialize the object output and wrap in an `MetaConfigObject`
                    object_output
                        .body
                        .unwrap()
                        .concat2()
                        .map_err(|e| {
                            error!("concatenating body: {:?}", e);
                            ()
                        })
                        .map(move |bytes| {
                            let result = match String::from_utf8(bytes.to_vec()) {
                                Ok(d) => d,
                                Err(e) => {
                                    println!("error!{:?}", e);
                                    return MetaConfigObject::Unknown;
                                }
                            };
                            let parts: Vec<&str> = file_key_clone
                                .trim_start_matches("minsql/meta/")
                                .split("/")
                                .collect();
                            let meta_obj = match (parts.len(), parts[0]) {
                                (2, "logs") => match serde_json::from_str(&result) {
                                    Ok(t) => MetaConfigObject::Log(t),
                                    Err(_) => MetaConfigObject::Unknown,
                                },
                                (2, "datastores") => match serde_json::from_str(&result) {
                                    Ok(t) => MetaConfigObject::DataStore(t),
                                    Err(_) => MetaConfigObject::Unknown,
                                },
                                (2, "tokens") => match serde_json::from_str(&result) {
                                    Ok(t) => MetaConfigObject::Token(t),
                                    Err(_) => MetaConfigObject::Unknown,
                                },
                                (3, "auth") => match serde_json::from_str(&result) {
                                    Ok(t) => MetaConfigObject::LogAuth((
                                        parts[1].to_string(),
                                        parts[2].to_string(),
                                        t,
                                    )),
                                    Err(_) => MetaConfigObject::Unknown,
                                },
                                _ => MetaConfigObject::Unknown,
                            };
                            meta_obj
                        })
                })
        })
        .buffer_unordered(5)
        .map(move |mco: MetaConfigObject| {
            //get a write lock on config
            let mut cfg_write = main_cfg.write().unwrap();
            //time to update the configuration!
            match mco {
                MetaConfigObject::Log(l) => {
                    cfg_write.log.insert(l.clone().name.unwrap(), l);
                }
                MetaConfigObject::DataStore(ds) => {
                    cfg_write.datastore.insert(ds.clone().name.unwrap(), ds);
                }
                MetaConfigObject::Token(t) => {
                    cfg_write.tokens.insert(t.access_key.clone(), t);
                }
                MetaConfigObject::LogAuth((token, log_name, log_auth)) => {
                    // Get the map for the token, if it's not set yet, initialize it.
                    let auth_logs = match cfg_write.auth.entry(token) {
                        Entry::Occupied(o) => o.into_mut(),
                        Entry::Vacant(v) => v.insert(HashMap::new()),
                    };
                    auth_logs.insert(log_name, log_auth);
                }
                _ => (),
            }

            drop(cfg_write);
        })
        .fold((), |_, _| Ok(()));

        task
    }

    pub fn monitor_metabucket(&self) {
        let read_cfg = self.config.read().unwrap();

        let metadata_bucket = read_cfg.server.metadata_bucket.clone();
        let metadata_endpoint = read_cfg.server.metadata_endpoint.clone();
        let access_key = read_cfg.server.access_key.clone();
        let secret_key = read_cfg.server.secret_key.clone();
        drop(read_cfg);

        let mut c = minio::Client::new(&metadata_endpoint).expect("Could not connect metabucket");
        c.set_credentials(Credentials::new(&access_key, &secret_key));

        let cfg = Arc::clone(&self.config);
        let task = c
            .listen_bucket_notification(
                &metadata_bucket,
                None,
                None,
                vec![
                    "s3:ObjectCreated:*".to_string(),
                    "s3:ObjectRemoved:*".to_string(),
                ],
            )
            .map_err(|_| ())
            .for_each(move |x| {
                for record in x.records {
                    let cfg = Arc::clone(&cfg);

                    let object_key = record.s3.object.key.replace("%2F", "/");
                    if record.event_name.starts_with("s3:ObjectCreated") {
                        load_config_for_key(cfg, object_key);
                    } else if record.event_name.starts_with("s3:ObjectRemoved:Delete") {
                        remove_config_for_key(cfg, object_key);
                    }
                }
                Ok(())
            });

        hyper::rt::spawn(task);
    }
}

/// Loads a configuration from the metabucket via object key, if it's a loaded type it will be
/// stored on the configuration.
fn load_config_for_key(cfg: Arc<RwLock<Config>>, object_key: String) {
    let cfg2 = Arc::clone(&cfg);
    // Get datastore for metabucket and create a client
    let ds = ds_for_metabucket(cfg);
    let s3_client = storage::client_for_datastore(&ds);

    let file_key_clone = object_key.clone();

    let sub_task = s3_client
        .get_object(GetObjectRequest {
            bucket: ds.bucket.clone(),
            key: object_key,
            ..Default::default()
        })
        .map_err(|e| {
            error!("getting object: {:?}", e);
            ()
        })
        .and_then(move |object_output| {
            // Deserialize the object output
            let cfg2 = Arc::clone(&cfg2);
            object_output
                .body
                .unwrap()
                .concat2()
                .map_err(|e| {
                    error!("concatenating body: {:?}", e);
                    ()
                })
                .and_then(move |bytes| {
                    let result = String::from_utf8(bytes.to_vec()).unwrap();

                    let parts: Vec<&str> = file_key_clone
                        .trim_start_matches("minsql/meta/")
                        .split("/")
                        .collect();
                    match (parts.len(), parts[0]) {
                        (2, "logs") => match serde_json::from_str(&result) {
                            Ok(log) => {
                                let mut cfg_write = cfg2.write().unwrap();
                                info!("Loading log: {}", &parts[1]);
                                cfg_write.log.insert(parts[1].to_string(), log);
                                drop(cfg_write);
                            }
                            Err(e) => {
                                error!("error loading log configuration {}", e);
                            }
                        },
                        (2, "datastores") => match serde_json::from_str(&result) {
                            Ok(datastore) => {
                                let mut cfg_write = cfg2.write().unwrap();
                                info!("Loading datastore: {}", &parts[1]);
                                cfg_write.datastore.insert(parts[1].to_string(), datastore);
                                drop(cfg_write);
                            }
                            Err(e) => {
                                error!("error loading datastore configuration {}", e);
                            }
                        },
                        (2, "tokens") => match serde_json::from_str(&result) {
                            Ok(token) => {
                                let mut cfg_write = cfg2.write().unwrap();
                                info!("Loading token: {}", &parts[1]);
                                cfg_write.tokens.insert(parts[1].to_string(), token);
                                drop(cfg_write);
                            }
                            Err(e) => {
                                error!("error loading datastore configuration {}", e);
                            }
                        },
                        (3, "auth") => match serde_json::from_str(&result) {
                            Ok(log_auth) => {
                                let mut cfg_write = cfg2.write().unwrap();
                                info!("Loading auth: {}", &parts[1]);
                                let auth_logs = match cfg_write.auth.entry(parts[1].to_string()) {
                                    Entry::Occupied(o) => o.into_mut(),
                                    Entry::Vacant(v) => v.insert(HashMap::new()),
                                };
                                auth_logs.insert(parts[2].to_string(), log_auth);
                                drop(cfg_write);
                            }
                            Err(e) => {
                                error!("error loading auth configuration {}", e);
                            }
                        },
                        _ => (),
                    };
                    Ok(())
                })
        });
    hyper::rt::spawn(sub_task);
}

/// Attemps to remove a configuration by object key
fn remove_config_for_key(cfg: Arc<RwLock<Config>>, object_key: String) {
    let parts: Vec<&str> = object_key
        .trim_start_matches("minsql/meta/")
        .split("/")
        .collect();
    match (parts.len(), parts[0]) {
        (2, "logs") => {
            let mut cfg_write = cfg.write().unwrap();
            info!("Removing log: {}", &parts[1]);
            cfg_write.log.remove(parts[1]);
            drop(cfg_write);
        }
        (2, "datastores") => {
            let mut cfg_write = cfg.write().unwrap();
            info!("Removing datastore: {}", &parts[1]);
            cfg_write.datastore.remove(parts[1]);
            drop(cfg_write);
        }
        (3, "auth") => {
            let mut cfg_write = cfg.write().unwrap();
            info!("Removing auth: {}", &parts[1]);
            let auth_logs = match cfg_write.auth.entry(parts[1].to_string()) {
                Entry::Occupied(o) => o.into_mut(),
                Entry::Vacant(v) => v.insert(HashMap::new()),
            };
            auth_logs.remove(parts[2]);
            drop(cfg_write);
        }
        _ => (),
    };
}

pub fn ds_for_metabucket(cfg: Arc<RwLock<Config>>) -> DataStore {
    // TODO: Maybe cache this on cfg.server
    let read_cfg = cfg.read().unwrap();
    // Represent the metabucket as a datastore to re-use other functions we have in `storage.rs`
    DataStore {
        endpoint: read_cfg.server.metadata_endpoint.clone(),
        access_key: read_cfg.server.access_key.clone(),
        secret_key: read_cfg.server.secret_key.clone(),
        bucket: read_cfg.server.metadata_bucket.clone(),
        prefix: "".to_owned(),
        name: Some("metabucket".to_owned()),
    }
}

#[derive(Debug)]
enum MetaConfigObject {
    Log(Log),
    DataStore(DataStore),
    LogAuth((String, String, LogAuth)),
    Token(Token),
    Unknown,
}
