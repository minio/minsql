// MinSQL
// Copyright (C) 2019  MinIO
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
//#![deny(warnings)]
extern crate futures;
extern crate hyper;
extern crate hyperscan;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate pretty_env_logger;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate toml;

use std::collections::HashMap;
use std::process;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use futures::{future, Future, Stream};
use hyper::Server;
use hyper::service::service_fn;
use tokio::timer::Interval;

use crate::config::Config;
use crate::ingest::flush_buffer;
use crate::ingest::IngestBuffer;

//use std::sync::Arc;


mod constants;
mod config;
mod http;
mod storage;
mod query;
mod dialect;
mod ingest;


fn main() {
    pretty_env_logger::init();
    // Load the configuration file
    let cfg = match config::load_configuration() {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            process::exit(0x0100);
        }
    };

    // Validate all datastore for reachability
    for ds in cfg.datastore.iter() {
        // if we find a bad datastore, for now let's panic
        if storage::can_reach_datastore(ds) == false {
            error!("{} is not a reachable datastore", ds.name.clone().unwrap());
            process::exit(0x0100);
        }
    }

    info!("Starting MinSQL Server");
    // initialize ingest buffers
    let mut log_ingest_buffers_map: HashMap<String, Mutex<IngestBuffer>> = HashMap::new();

    // for each log, initialize an ingest buffer
    for log in &cfg.log {
        log_ingest_buffers_map.insert(log.name.clone(), Mutex::new(IngestBuffer::new()));
    }

    let log_ingest_buffers: Arc<HashMap<String, Mutex<IngestBuffer>>> = Arc::new(log_ingest_buffers_map);
    // create a referece to the hashmap that we will share across intervals below
    let ingest_buffer_interval = Arc::clone(&log_ingest_buffers);


    let addr = cfg.server.as_ref().unwrap().address.as_ref().unwrap().parse().unwrap();

    let cfg = Box::new(cfg);
    let cfg: &'static _ = Box::leak(cfg);

    hyper::rt::run(future::lazy(move || {

        // for each log, start an interval to flush data at window speed, as long as the
        // commit window is not 0
        for log in &cfg.log {
            let ingest_buffer2 = Arc::clone(&ingest_buffer_interval);
            if log.commit_window != "0" {
                let log_name = log.name.clone();
                info!("Starting flusing loop for {} at {}", &log_name, &log.commit_window);

                let task = Interval::new(Instant::now(), Duration::from_secs(Config::commit_window_to_seconds(&log.commit_window)))
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
        // Hyper Service Function that will serve each request as a new task
        let new_service = move || {
            let log_ingest_buffers = Arc::clone(&log_ingest_buffers);
            // Move a clone of `configuration` into the `service_fn`.
            service_fn(move |req| {
                let log_ingest_buffers = Arc::clone(&log_ingest_buffers);
                http::request_router(req, &cfg, log_ingest_buffers)
            })
        };

        let server = Server::bind(&addr)
            .serve(new_service)
            .map_err(|e| eprintln!("server error: {}", e));

        info!("Listening on http://{}", addr);

        server
    }));
}