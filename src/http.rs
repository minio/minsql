use futures::{ Future, Stream};
use hyper::{Body, Chunk, Client, header, Method, Request, Response, StatusCode};
use hyper::client::HttpConnector;

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
pub type ResponseFuture = Box<Future<Item=Response<Body>, Error=GenericError> + Send>;

static URL: &str = "http://127.0.0.1:1337/json_api";
static POST_DATA: &str = r#"{"original": "data"}"#;

//use crate::config::Config;

pub fn api_log_put_response(req: Request<Body>) -> ResponseFuture {
    info!("Logging data");
    Box::new(req.into_body()
        .concat2() // Concatenate all chunks in the body
        .from_err()
        .and_then(|entire_body| {
            // Read the body from the request
            let payload: String = match String::from_utf8(entire_body.to_vec()) {
                Ok(str) => str,
                Err(err) => panic!("Couldn't convert buffer to string: {}", err)
            };
            println!("Log Data:\n{}", payload);
            // Send response that the request has been received successfully
            let response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from("ok"))?;
            Ok(response)
        })
    )
}

pub fn client_request_response(client: &Client<HttpConnector>) -> ResponseFuture {
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

pub fn api_post_response(req: Request<Body>) -> ResponseFuture {
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

