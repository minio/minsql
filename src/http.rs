use futures::{future, Future, Stream};
use hyper::{Body, Chunk, Client, header, Method, Request, Response, StatusCode};
use hyper::client::HttpConnector;

use crate::config::Config;
use crate::storage::write_to_datastore;

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
pub type ResponseFuture = Box<Future<Item=Response<Body>, Error=GenericError> + Send>;

static URL: &str = "http://127.0.0.1:1337/json_api";
static POST_DATA: &str = r#"{"original": "data"}"#;


static INDEX: &[u8] = b"<a href=\"test.html\">test.html</a>";
static NOTFOUND: &[u8] = b"Not Found";

pub fn request_router(req: Request<Body>, client: &Client<HttpConnector>, cfg: &Config) -> ResponseFuture {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") | (&Method::GET, "/index.html") => {
            let body = Body::from(INDEX);
            Box::new(future::ok(Response::new(body)))
        }
        (&Method::GET, "/test.html") => {
            client_request_response(client)
        }
        (&Method::POST, "/mylog/search") => {
            api_post_response(req)
        }
        (&Method::PUT, "/mylog/store") => {
            api_log_put_response(cfg, req)
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

fn api_log_put_response(cfg: &Config, req: Request<Body>) -> ResponseFuture {
    info!("Logging data");
    // make a clone of the config for the closure
    let cfg = cfg.clone();
    Box::new(req.into_body()
        .concat2() // Concatenate all chunks in the body
        .from_err()
        .and_then(move |entire_body| {
            // Read the body from the request
            let payload: String = match String::from_utf8(entire_body.to_vec()) {
                Ok(str) => str,
                Err(err) => panic!("Couldn't convert buffer to string: {}", err)
            };
            println!("Log Data:\n{}", payload);
            write_to_datastore(&"mylog", &cfg.datastore[0], &payload);
            // Send response that the request has been received successfully
            let response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from("ok"))?;
            Ok(response)
        })
    )
}

fn client_request_response(client: &Client<HttpConnector>) -> ResponseFuture {
    let req = Request::builder()
        .method(Method::POST)
        .uri(URL)
        .header(header::CONTENT_TYPE, "application/json")
        .body(POST_DATA.into())
        .unwrap();

    Box::new(client.request(req).from_err().map(|web_res| {
        // Compare the JSON we sent (before) with what we received (after):
        let body = Body::wrap_stream(web_res.into_body().map(|b| {
            Chunk::from(format!("<b>POST request body</b>: {}<br><b>Response</b>: {}",
                                POST_DATA,
                                std::str::from_utf8(&b).unwrap()))
        }));

        Response::new(body)
    }))
}

fn api_post_response(req: Request<Body>) -> ResponseFuture {
    // A web api to run against
    Box::new(req.into_body()
        .concat2() // Concatenate all chunks in the body
        .from_err()
        .and_then(|entire_body| {
            // TODO: Replace all unwraps with proper error handling
            let str = String::from_utf8(entire_body.to_vec())?;
            let mut data: serde_json::Value = serde_json::from_str(&str)?;
            data["test"] = serde_json::Value::from("test_value");
            let json = serde_json::to_string(&data)?;
            let response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json))?;
            Ok(response)
        })
    )
}

