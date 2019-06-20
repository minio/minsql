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
#[allow(unused)]
#[macro_use]
extern crate bitflags;

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use std::process;
use std::sync::Mutex;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::time::Instant;

use futures::{future, Future, Stream};
use hyper::server::conn::Http;
use hyper::service::service_fn;
use hyper::Server;
use log::{error, info};
use native_tls::{Identity, TlsAcceptor};
use tokio::net::TcpListener;
use tokio::timer::{Delay, Interval};

use crate::config::Config;
use crate::ingest::{Ingest, IngestBuffer};
use crate::meta::Meta;

mod auth;
mod config;
mod constants;
mod dialect;
mod filter;
mod http;
mod ingest;
mod meta;
mod query;
mod storage;

pub struct Bootstrap {}

pub fn bootstrap() {
    // Load the configuration file
    let cfg = match config::load_configuration() {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            process::exit(0x0100);
        }
    };
    let cfg = Arc::new(RwLock::new(cfg));

    // Start minSQL
    let minsql_c = MinSQL::new(cfg);
    minsql_c.run();
}

pub struct MinSQL {
    config: Arc<RwLock<Config>>,
}

impl MinSQL {
    pub fn new(cfg: Arc<RwLock<Config>>) -> MinSQL {
        MinSQL { config: cfg }
    }

    pub fn run(&self) {
        // make sure all datastores shown are reachable
        let cfg_valid_ds = Arc::clone(&self.config);
        self.validate_datastore_reachability(cfg_valid_ds);

        let meta_cfg = Arc::clone(&self.config);
        // initial load of configuraiton
        tokio::run(future::lazy(|| {
            println!("running future");
            let meta_c = Meta::new(meta_cfg);
            meta_c.load_config_from_metabucket();

            let when = Instant::now() + Duration::from_millis(100);
            let task = Delay::new(when)
                .and_then(|_| {
                    println!("Hello world!");
                    Ok(())
                })
                .map_err(|e| panic!("delay errored; err={:?}", e));

            task
        }));

        info!("Starting MinSQL Server");
        // initialize ingest buffers
        let mut log_ingest_buffers_map: HashMap<String, Mutex<IngestBuffer>> = HashMap::new();

        // for each log, initialize an ingest buffer
        for (log_name, _) in &self.config.read().unwrap().log {
            log_ingest_buffers_map.insert(log_name.clone(), Mutex::new(IngestBuffer::new()));
        }

        let log_ingest_buffers: Arc<HashMap<String, Mutex<IngestBuffer>>> =
            Arc::new(log_ingest_buffers_map);
        // create a referece to the hashmap that we will share across intervals below
        let ingest_buffer_interval = Arc::clone(&log_ingest_buffers);

        let addr = self
            .config
            .read()
            .unwrap()
            .get_server_address()
            .parse()
            .unwrap();

        let service_cfg = Arc::clone(&self.config);
        // Hyper Service Function that will serve each request as a new task
        let new_service = move || {
            let log_ingest_buffers = Arc::clone(&log_ingest_buffers);
            let inner_service_cfg = Arc::clone(&service_cfg);

            let http_c = http::Http::new(inner_service_cfg);
            // Move a clone of `configuration` into the `service_fn`.
            service_fn(move |req| {
                let log_ingest_buffers = Arc::clone(&log_ingest_buffers);
                http_c.request_router(req, log_ingest_buffers)
            })
        };
        let read_cfg = self.config.read().unwrap();

        let server_cfg = match &read_cfg.server {
            Some(s) => s,
            None => panic!("No server configuration in your config.toml"),
        };

        match (&server_cfg.pkcs12_cert, &server_cfg.pkcs12_password) {
            (Some(pkcs12_cert), Some(pkcs12_pass)) => {
                // HTTPS server
                let mut der = Vec::new();

                // Read cert file into der
                File::open(&pkcs12_cert[..])
                    .expect("PKCS12 cert not found")
                    .read_to_end(&mut der)
                    .expect("Could not read file");

                let cert = Identity::from_pkcs12(&der, &pkcs12_pass[..]).unwrap();

                let tls_cx = TlsAcceptor::builder(cert).build().unwrap();
                let tls_cx = tokio_tls::TlsAcceptor::from(tls_cx);

                // Instance responsable for flushing ingestion buffers
                let minsql_c = MinSQL::new(Arc::clone(&self.config));

                hyper::rt::run(future::lazy(move || {
                    minsql_c.start_ingestion_flush_task(ingest_buffer_interval);

                    let srv = TcpListener::bind(&addr).expect("Error binding local port");
                    // Use lower lever hyper API to be able to intercept client connection
                    let http_proto = Http::new();
                    let server = http_proto
                        .serve_incoming(
                            srv.incoming().and_then(move |socket| {
                                tls_cx
                                    .accept(socket)
                                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                            }),
                            new_service,
                        )
                        .then(|res| match res {
                            Ok(conn) => Ok(Some(conn)),
                            Err(e) => {
                                eprintln!("Accept Connection Error: {}", e);
                                Ok(None)
                            }
                        })
                        .for_each(|conn_opt| {
                            if let Some(conn) = conn_opt {
                                hyper::rt::spawn(
                                    conn.and_then(|c| c.map_err(|e| panic!("Hyper error {}", e)))
                                        .map_err(|e| eprintln!("Connection error {}", e)),
                                );
                            }

                            Ok(())
                        });

                    info!("Listening on https://{}", addr);

                    server
                }));
            }
            (None, None) => {
                // Instance responsable for flushing ingestion buffers
                let minsql_c = MinSQL::new(Arc::clone(&self.config));
                // HTTP server
                hyper::rt::run(future::lazy(move || {
                    minsql_c.start_ingestion_flush_task(ingest_buffer_interval);

                    let server = Server::bind(&addr)
                        .serve(new_service)
                        .map_err(|e| eprintln!("server error: {}", e));
                    info!("Listening on http://{}", addr);
                    server
                }));
            }
            _ => panic!("PKCS12 cert or password is missing"),
        }
    }
    fn start_ingestion_flush_task(&self, ingest_buffer: Arc<HashMap<String, Mutex<IngestBuffer>>>) {
        let read_cfg = self.config.read().unwrap();

        // for each log, start an interval to flush data at window speed, as long as the
        // commit window is not 0
        for (log_name, log) in &read_cfg.log {
            let ingest_buffer2 = Arc::clone(&ingest_buffer);
            if log.commit_window != "0" {
                // What the flush spawn will take with him
                let cfg = Arc::clone(&self.config);
                let ingest_c = Ingest::new(cfg);

                let log_name = log_name.clone();
                info!(
                    "Starting flusing loop for {} at {}",
                    &log_name, &log.commit_window
                );
                let task = Interval::new(
                    Instant::now(),
                    Duration::from_secs(Config::commit_window_to_seconds(&log.commit_window)),
                )
                .for_each(move |_| {
                    let ingest_buffer3 = Arc::clone(&ingest_buffer2);
                    let log_name = log_name.clone();
                    ingest_c.flush_buffer(&log_name, ingest_buffer3);
                    Ok(())
                })
                .map_err(|e| panic!("interval errored; err={:?}", e));
                hyper::rt::spawn(task);
            }
        }
    }

    /// Validate all datastore for reachability
    fn validate_datastore_reachability(&self, cfg: Arc<RwLock<Config>>) {
        let read_cfg = cfg.read().unwrap();
        for (ds_name, ds) in read_cfg.datastore.iter() {
            // if we find a bad datastore, for now let's panic
            if storage::can_reach_datastore(&ds) == false {
                error!("{} is not a reachable datastore", &ds_name);
                process::exit(0x0100);
            }
        }
    }
}
