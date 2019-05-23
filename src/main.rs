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

//#![deny(warnings)]
extern crate futures;
extern crate hyper;
#[macro_use]
extern crate log;
extern crate pretty_env_logger;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate toml;
extern crate hyperscan;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate bitflags;

use std::process;

use futures::{future, Future};
use hyper::{Client, Server};
use hyper::service::service_fn;
use std::sync::Arc;

mod constants;
mod config;
mod http;
mod storage;
mod query;
mod dialect;



fn main() {
    pretty_env_logger::init();
    // Load the configuration file
    let configuration = match config::load_configuration() {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            process::exit(0x0100);
        }
    };

    // Validate all datastore for reachability
    for ds in configuration.datastore.iter() {
        // if we find a bad datastore, for now let's panic
        if storage::can_reach_datastore(ds) == false {
            error!("{} is not a reachable datastore", ds.name.clone().unwrap());
            process::exit(0x0100);
        }
    }

    info!("Starting MinSQL Server");

//    let addr = "0.0.0.0:9999".parse().unwrap();
    let addr = configuration.server.as_ref().unwrap().address.as_ref().unwrap().parse().unwrap();

    let cfg = Arc::new(configuration);

    hyper::rt::run(future::lazy(move || {
        // Share a `Client` with all `Service`s
        let client = Client::new();

        let new_service = move || {
            // Move a clone of `client` into the `service_fn`.
            let client = client.clone();
            // Move a clone of `configuration` into the `service_fn`.
            let cfg = cfg.clone();

            service_fn(move |req| {
                http::request_router(req, &client, &cfg)
            })
        };

        let server = Server::bind(&addr)
            .serve(new_service)
            .map_err(|e| eprintln!("server error: {}", e));

        info!("Listening on http://{}", addr);

        server
    }));
}