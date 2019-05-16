#![deny(warnings)]
extern crate futures;
extern crate hyper;
#[macro_use]
extern crate log;
extern crate pretty_env_logger;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate toml;


use std::process;

use futures::{future, Future};
use hyper::{Body, Client, Method, Request, Response, Server, StatusCode};
use hyper::client::HttpConnector;
use hyper::service::service_fn;

//use serde::{Deserialize};

mod config;
mod http;
mod storage;


static INDEX: &[u8] = b"<a href=\"test.html\">test.html</a>";
static NOTFOUND: &[u8] = b"Not Found";

fn request_router(req: Request<Body>, client: &Client<HttpConnector>, cfg: &config::Config) -> http::ResponseFuture {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") | (&Method::GET, "/index.html") => {
            let body = Body::from(INDEX);
            Box::new(future::ok(Response::new(body)))
        }
        (&Method::GET, "/test.html") => {
            http::client_request_response(client)
        }
        (&Method::POST, "/mylog/search") => {
            http::api_post_response(req)
        }
        (&Method::PUT, "/mylog/store") => {
            http::api_log_put_response(cfg, req)
        }
        _ => {
            // Return 404 not found response.
            let body = Body::from(NOTFOUND);
            Box::new(future::ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(body)
                .unwrap()))
        }
    }
}


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

    let addr = "0.0.0.0:9999".parse().unwrap();

    hyper::rt::run(future::lazy(move || {
        // Share a `Client` with all `Service`s
        let client = Client::new();

        let new_service = move || {
            // Move a clone of `client` into the `service_fn`.
            let client = client.clone();
            // Move a clone of `configuration` into the `service_fn`.
            let cfg = configuration.clone();

            service_fn(move |req| {
                request_router(req, &client, &cfg)
            })
        };

        let server = Server::bind(&addr)
            .serve(new_service)
            .map_err(|e| eprintln!("server error: {}", e));

        info!("Listening on http://{}", addr);

        server
    }));
}