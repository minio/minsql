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

#[macro_use]
extern crate bitflags;
extern crate futures;
extern crate hyper;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate native_tls;
extern crate pretty_env_logger;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tokio_tls;
extern crate toml;

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::process;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use futures::{future, Future, Stream};
use hyper::server::conn::Http;
use hyper::service::service_fn;
use hyper::Server;
use native_tls::{Identity, TlsAcceptor};
use tokio::net::TcpListener;
use tokio::timer::Interval;

use crate::config::Config;
use crate::ingest::flush_buffer;
use crate::ingest::IngestBuffer;

mod auth;
mod config;
mod constants;
mod dialect;
mod http;
mod ingest;
mod query;
mod storage;

pub fn run() {
    // Load the configuration file
    let cfg = match config::load_configuration() {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            process::exit(0x0100);
        }
    };

    // Validate all datastore for reachability
    for (ds_name, ds) in cfg.datastore.iter() {
        // if we find a bad datastore, for now let's panic
        if storage::can_reach_datastore(&ds) == false {
            error!("{} is not a reachable datastore", &ds_name);
            process::exit(0x0100);
        }
    }

    info!("Starting MinSQL Server");
    // initialize ingest buffers
    let mut log_ingest_buffers_map: HashMap<String, Mutex<IngestBuffer>> = HashMap::new();

    // for each log, initialize an ingest buffer
    for (log_name, _) in &cfg.log {
        log_ingest_buffers_map.insert(log_name.clone(), Mutex::new(IngestBuffer::new()));
    }

    let log_ingest_buffers: Arc<HashMap<String, Mutex<IngestBuffer>>> =
        Arc::new(log_ingest_buffers_map);
    // create a referece to the hashmap that we will share across intervals below
    let ingest_buffer_interval = Arc::clone(&log_ingest_buffers);

    let addr = cfg
        .server
        .as_ref()
        .unwrap()
        .address
        .as_ref()
        .unwrap()
        .parse()
        .unwrap();

    let cfg = Box::new(cfg);
    let cfg: &'static _ = Box::leak(cfg);

    // Hyper Service Function that will serve each request as a new task
    let new_service = move || {
        let log_ingest_buffers = Arc::clone(&log_ingest_buffers);
        // Move a clone of `configuration` into the `service_fn`.
        service_fn(move |req| {
            let log_ingest_buffers = Arc::clone(&log_ingest_buffers);
            http::request_router(req, &cfg, log_ingest_buffers)
        })
    };

    if cfg.server.is_some() && cfg.server.as_ref().unwrap().pkcs12_cert.is_some() {
        let server = match &cfg.server {
            Some(s) => s,
            None => panic!("No server configuration"),
        };

        let pkcs12_cert = match &server.pkcs12_cert {
            Some(v) => v.clone(),
            None => panic!("Missing pcks12 path"),
        };
        let pkcs12_pass = match &server.pkcs12_password {
            Some(v) => v.clone(),
            None => panic!("Missing pcks12 password"),
        };

        let mut f = match File::open(&pkcs12_cert[..]) {
            Ok(v) => v,
            Err(e) => {
                println!("PKCS12 Cert not found: {:?}", e);
                process::exit(0x0100);
            }
        };

        let mut der = Vec::new();
        // read the whole file
        match f.read_to_end(&mut der) {
            Ok(_) => (),
            Err(e) => {
                println!("Could not read file: {:?}", e);
                process::exit(0x0100);
            }
        };

        let cert = Identity::from_pkcs12(&der, &pkcs12_pass[..]).unwrap();

        let tls_cx = TlsAcceptor::builder(cert).build().unwrap();
        let tls_cx = tokio_tls::TlsAcceptor::from(tls_cx);

        hyper::rt::run(future::lazy(move || {
            start_ingestion_flush_task(&cfg, ingest_buffer_interval);

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
                        eprintln!("Error: {}", e);
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
    } else {
        hyper::rt::run(future::lazy(move || {
            start_ingestion_flush_task(&cfg, ingest_buffer_interval);

            let server = Server::bind(&addr)
                .serve(new_service)
                .map_err(|e| eprintln!("server error: {}", e));
            info!("Listening on http://{}", addr);
            server
        }));
    }
}

fn start_ingestion_flush_task(
    cfg: &'static Config,
    ingest_buffer: Arc<HashMap<String, Mutex<IngestBuffer>>>,
) {
    // for each log, start an interval to flush data at window speed, as long as the
    // commit window is not 0
    for (log_name, log) in &cfg.log {
        let ingest_buffer2 = Arc::clone(&ingest_buffer);
        if log.commit_window != "0" {
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
                flush_buffer(&log_name, &cfg, ingest_buffer3);
                Ok(())
            })
            .map_err(|e| panic!("interval errored; err={:?}", e));
            hyper::rt::spawn(task);
        }
    }
}
