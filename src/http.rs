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
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::time::Instant;

use futures::{future, Future, Stream};
use hyper::{Body, Chunk, Client, header, Method, Request, Response, StatusCode};
use hyper::client::HttpConnector;
use regex::Regex;

use crate::config::Config;
use crate::query::parse_query;
use crate::query::ScanFlags;
use crate::query::scanlog;
use crate::storage::{list_msl_bucket_files, write_to_datastore};
use crate::storage::read_file;

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
pub type ResponseFuture = Box<Future<Item=Response<Body>, Error=GenericError> + Send>;

static URL: &str = "http://127.0.0.1:1337/json_api";
static POST_DATA: &str = r#"{"original": "data"}"#;


static INDEX: &[u8] = b"MinSQL";
static NOTFOUND: &[u8] = b"Not Found";

#[derive(Debug)]
struct PositionalColumn {
    position: i32,
    alias: String,
}

#[derive(Debug)]
struct SmartColumn {
    // $ip, $email...
    typed: String,
    // for $ip or $ip1 is 1, for $ip2 is 2 ...
    position: i32,
    // if this column was aliased
    alias: String,
}

#[derive(Debug)]
struct RequestedLog {
    name: String,
    method: String,
}

#[derive(Debug)]
pub struct RequestedLogError {
    details: String
}

impl RequestedLogError {
    pub fn new(msg: &str) -> RequestedLogError {
        RequestedLogError { details: msg.to_string() }
    }
}

impl fmt::Display for RequestedLogError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

fn return_404() -> Response<Body> {
    let body = Body::from(NOTFOUND);
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(body)
        .unwrap()
}

// Return 404 not found response.
fn return_404_future() -> ResponseFuture {
    Box::new(future::ok(return_404()))
}

fn requested_log_from_request(req: &Request<Body>) -> Result<RequestedLog, RequestedLogError> {
    let request_path_no_slash = String::from(&req.uri().path()[1..]);
    let path_split = request_path_no_slash.split("/");
    let parts: Vec<&str> = path_split.collect();
    if parts.len() != 2 {
        return Err(RequestedLogError::new("Invalid log structure"));
    }
    let logname = parts[0].to_string();
    let method = parts[1].to_string();
    return Ok(RequestedLog { name: logname, method: method });
}

pub fn request_router(req: Request<Body>, client: &Client<HttpConnector>, cfg: &Config) -> ResponseFuture {
    // handle GETs as their own thing
    if req.method() == &Method::GET {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/") | (&Method::GET, "/index.html") => {
                let body = Body::from(INDEX);
                Box::new(future::ok(Response::new(body)))
            }
            (&Method::GET, "/test.html") => {
                client_request_response(client)
            }
            _ => {
                // Return 404 not found response.
                return_404_future()
            }
        }
    } else if req.method() == &Method::POST {
        match (req.method(), req.uri().path()) {
            (&Method::POST, "/search") => {
                api_log_search(cfg, req)
            }
            _ => {
                // Return 404 not found response.
                return_404_future()
            }
        }
    } else {
        //request path without the /
        let requested_log = match requested_log_from_request(&req) {
            Ok(ln) => ln,
            Err(e) => {
                error!("Failed to load configuration: {}", e);
                return return_404_future();
            }
        };

        // is this a valid requested_log? else reject
        match cfg.get_log(&requested_log.name) {
            Some(_) => {} // if we get a log it's valid
            _ => {
                info!("Attemped access of unknow log {}", requested_log.name);
                return return_404_future();
            }
        }

        match (req.method(), &requested_log.method[..]) {
            (&Method::PUT, "store") => {
                api_log_store(cfg, req)
            }
            _ => {
                // Return 404 not found response.
                return return_404_future();
            }
        }
    }
}

// Handles a PUT operation to a log
fn api_log_store(cfg: &Config, req: Request<Body>) -> ResponseFuture {
    let requested_log = match requested_log_from_request(&req) {
        Ok(ln) => ln,
        Err(e) => {
            error!("{}", e);
            return return_404_future();
        }
    };
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
            match write_to_datastore(&requested_log.name, &cfg.datastore[0], &payload) {
                Ok(x) => x,
                Err(e) => {
                    error!("{}", e);
                    let response = Response::builder()
                        .status(StatusCode::INSUFFICIENT_STORAGE)
                        .header(header::CONTENT_TYPE, "text/plain")
                        .body(Body::from("fail"))?;
                    return Ok(response);
                }
            };

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

// performs a query on a log
fn api_log_search(cfg: &Config, req: Request<Body>) -> ResponseFuture {
    let start = Instant::now();
    lazy_static! {
        static ref SMART_FIELDS_RE : Regex = Regex::new(r"((\$(ip|email|date|url|quoted))([0-9]+)*)\b").unwrap();
    };

    // make a clone of the config for the closure
    let cfg = cfg.clone();
    // A web api to run against
    Box::new(req.into_body()
        .concat2() // Concatenate all chunks in the body
        .from_err()
        .and_then(parse_query)
//        .from_err()
        .map_err(|e| {
            error!("----{}",e);
            e
        })
        .and_then(move |ast| {
            println!("AST: {:?}", ast);
            // Validate all the tables for all the queries, we don't want to start serving content
            // for the first query and then discover subsequent queries are invalid
            for query in &ast {
                // find the table they want to query
                let some_table = match query {
                    sqlparser::sqlast::SQLStatement::SQLSelect(ref q) => {
                        match q.body {
                            sqlparser::sqlast::SQLSetExpr::Select(ref bodyselect) => {
                                bodyselect.relation.clone()
                            }
                            _ => {
                                error!("No table found");
                                let response = Response::builder()
                                    .status(StatusCode::BAD_REQUEST)
                                    .header(header::CONTENT_TYPE, "text/plain")
                                    .body(Body::from("fail"))?;
                                return Ok(response);
                            }
                        }
                    }
                    _ => {
                        error!("Not the type of query we support");
                        let response = Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .header(header::CONTENT_TYPE, "text/plain")
                            .body(Body::from("Unsupported query"))?;
                        return Ok(response);
                    }
                };
                if some_table == None {
                    error!("No table found");
                    let response = Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header(header::CONTENT_TYPE, "text/plain")
                        .body(Body::from("No table was found in the query statement"))?;
                    return Ok(response);
                }
                let table = some_table.unwrap().to_string();
                match cfg.get_log(&table) {
                    Some(_) => (),
                    _ => {
                        error!("Tried to search an unknow log");
                        return Ok(return_404());
                    }
                };
            }

            let mut resulting_data = String::new();
            // for each query, retrive data
            for query in &ast {
                // find the table they want to query
                let some_table = match query {
                    sqlparser::sqlast::SQLStatement::SQLSelect(ref q) => {
                        match q.body {
                            sqlparser::sqlast::SQLSetExpr::Select(ref bodyselect) => {
                                bodyselect.relation.clone()
                            }
                            _ => {
                                error!("No table found");
                                let response = Response::builder()
                                    .status(StatusCode::BAD_REQUEST)
                                    .header(header::CONTENT_TYPE, "text/plain")
                                    .body(Body::from("fail"))?;
                                return Ok(response);
                            }
                        }
                    }
                    _ => {
                        error!("Not the type of query we support");
                        let response = Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .header(header::CONTENT_TYPE, "text/plain")
                            .body(Body::from("Unsupported query"))?;
                        return Ok(response);
                    }
                };
                if some_table == None {
                    error!("No table found");
                    let response = Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header(header::CONTENT_TYPE, "text/plain")
                        .body(Body::from("No table was found in the query statement"))?;
                    return Ok(response);
                }
                let table = some_table.unwrap().to_string();
                let log = match cfg.get_log(&table) {
                    Some(l) => l,
                    _ => {
                        error!("Tried to search an unknow log");
                        return Ok(return_404());
                    }
                };

                // determine our read strategy
                let read_all = match query {
                    sqlparser::sqlast::SQLStatement::SQLSelect(ref q) => {
                        match q.body {
                            sqlparser::sqlast::SQLSetExpr::Select(ref bodyselect) => {
                                let mut is_wildcard = false;
                                for projection in &bodyselect.projection {
                                    if *projection == sqlparser::sqlast::SQLSelectItem::Wildcard {
                                        is_wildcard = true
                                    }
                                }
                                is_wildcard
                            }
                            _ => {
                                false
                            }
                        }
                    }
                    _ => {
                        false
                    }
                };

                let projections = match query {
                    sqlparser::sqlast::SQLStatement::SQLSelect(ref q) => {
                        match q.body {
                            sqlparser::sqlast::SQLSetExpr::Select(ref bodyselect) => {
                                bodyselect.projection.clone()
                            }
                            _ => {
                                Vec::new() //return empty
                            }
                        }
                    }
                    _ => {
                        Vec::new() //return empty
                    }
                };

                let mut positional_fields: Vec<PositionalColumn> = Vec::new();
                let mut smart_fields: Vec<SmartColumn> = Vec::new();
                let mut smart_fields_set: HashSet<String> = HashSet::new();
                let mut projections_ordered: Vec<String> = Vec::new();
                // TODO: We should stream the data out as it becomes available to save memory
                for proj in &projections {
                    match proj {
                        sqlparser::sqlast::SQLSelectItem::UnnamedExpression(ref ast) => {
                            // we have an identifier
                            match ast {
                                sqlparser::sqlast::ASTNode::SQLIdentifier(ref identifier) => {
                                    let id_name = &identifier[1..];
                                    let position = match id_name.parse::<i32>() {
                                        Ok(p) => p,
                                        Err(_) => -1
                                    };
                                    // if we were able to parse identifier as an i32 it's a positional
                                    if position > 0 {
                                        positional_fields.push(PositionalColumn { position: position, alias: identifier.clone() });
                                        projections_ordered.push(identifier.clone());
                                    } else {
                                        // try to parse as as smart field
                                        for sfield in SMART_FIELDS_RE.captures_iter(identifier) {
                                            let typed = sfield[2].to_string();
                                            let mut pos = 1;
                                            if sfield.get(4).is_none() == false {
                                                pos = match sfield[4].parse::<i32>() {
                                                    Ok(p) => p,
                                                    // technically this should never happen as the regex already validated an integer
                                                    Err(_) => -1,
                                                };
                                            }
                                            // we use this set to keep track of active smart fields
                                            smart_fields_set.insert(typed.clone());
                                            // track the smartfield
                                            smart_fields.push(SmartColumn { typed: typed.clone(), position: pos, alias: identifier.clone() });
                                            // record the order or extraction
                                            projections_ordered.push(identifier.clone());
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        _ => {} // for now let's not do anything on other Variances
                    }
                }

                // see which fields in the conditions were not requested in the projections and extract them too
                match query {
                    sqlparser::sqlast::SQLStatement::SQLSelect(ref q) => {
                        match q.body {
                            sqlparser::sqlast::SQLSetExpr::Select(ref bodyselect) => {
                                for slct in &bodyselect.selection {
                                    match slct {
                                        sqlparser::sqlast::ASTNode::SQLIsNotNull(ast) => {
                                            let identifier = match **ast {
                                                sqlparser::sqlast::ASTNode::SQLIdentifier(ref identifier) => {
                                                    identifier.to_string()
                                                }
                                                _ => {
                                                    // TODO: Should we be retunring anything at all?
                                                    "".to_string()
                                                }
                                            };
                                            //positional or smart?
                                            let id_name = &identifier[1..];
                                            let position = match id_name.parse::<i32>() {
                                                Ok(p) => p,
                                                Err(_) => -1
                                            };
                                            // if we were able to parse identifier as an i32 it's a positional
                                            if position > 0 {
                                                positional_fields.push(PositionalColumn { position: position, alias: identifier.clone() });
                                            } else {
                                                // try to parse as as smart field
                                                for sfield in SMART_FIELDS_RE.captures_iter(&identifier[..]) {
                                                    let typed = sfield[2].to_string();
                                                    let mut pos = 1;
                                                    if sfield.get(4).is_none() == false {
                                                        pos = match sfield[4].parse::<i32>() {
                                                            Ok(p) => p,
                                                            // technically this should never happen as the regex already validated an integer
                                                            Err(_) => -1,
                                                        };
                                                    }
                                                    // we use this set to keep track of active smart fields
                                                    smart_fields_set.insert(typed.clone());
                                                    // track the smartfield
                                                    smart_fields.push(SmartColumn { typed: typed.clone(), position: pos, alias: identifier.clone() });
                                                }
                                            }
                                        }
                                        sqlparser::sqlast::ASTNode::SQLIsNull(ast) => {
                                            let identifier = match **ast {
                                                sqlparser::sqlast::ASTNode::SQLIdentifier(ref identifier) => {
                                                    identifier.to_string()
                                                }
                                                _ => {
                                                    // TODO: Should we be retunring anything at all?
                                                    "".to_string()
                                                }
                                            };
                                            //positional or smart?
                                            let id_name = &identifier[1..];
                                            let position = match id_name.parse::<i32>() {
                                                Ok(p) => p,
                                                Err(_) => -1
                                            };
                                            // if we were able to parse identifier as an i32 it's a positional
                                            if position > 0 {
                                                positional_fields.push(PositionalColumn { position: position, alias: identifier.clone() });
                                            } else {
                                                // try to parse as as smart field
                                                for sfield in SMART_FIELDS_RE.captures_iter(&identifier[..]) {
                                                    let typed = sfield[2].to_string();
                                                    let mut pos = 1;
                                                    if sfield.get(4).is_none() == false {
                                                        pos = match sfield[4].parse::<i32>() {
                                                            Ok(p) => p,
                                                            // technically this should never happen as the regex already validated an integer
                                                            Err(_) => -1,
                                                        };
                                                    }
                                                    // we use this set to keep track of active smart fields
                                                    smart_fields_set.insert(typed.clone());
                                                    // track the smartfield
                                                    smart_fields.push(SmartColumn { typed: typed.clone(), position: pos, alias: identifier.clone() });
                                                }
                                            }
                                        }
                                        sqlparser::sqlast::ASTNode::SQLBinaryExpr { left, op: _, right: _ } => {
                                            let identifier = left.to_string();

                                            //positional or smart?
                                            let id_name = &identifier[1..];
                                            let position = match id_name.parse::<i32>() {
                                                Ok(p) => p,
                                                Err(_) => -1
                                            };
                                            // if we were able to parse identifier as an i32 it's a positional
                                            if position > 0 {
                                                positional_fields.push(PositionalColumn { position: position, alias: identifier.clone() });
                                            } else {
                                                // try to parse as as smart field
                                                for sfield in SMART_FIELDS_RE.captures_iter(&identifier[..]) {
                                                    let typed = sfield[2].to_string();
                                                    let mut pos = 1;
                                                    if sfield.get(4).is_none() == false {
                                                        pos = match sfield[4].parse::<i32>() {
                                                            Ok(p) => p,
                                                            // technically this should never happen as the regex already validated an integer
                                                            Err(_) => -1,
                                                        };
                                                    }
                                                    // we use this set to keep track of active smart fields
                                                    smart_fields_set.insert(typed.clone());
                                                    // track the smartfield
                                                    smart_fields.push(SmartColumn { typed: typed.clone(), position: pos, alias: identifier.clone() });
                                                }
                                            }
                                        }
                                        _ => {
                                            info!("Unhandled operation");
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                };

                // Build the parsing flags used by scanlog
                let mut scan_flags: ScanFlags = ScanFlags::NONE;
                for sfield_type in smart_fields_set {
                    let flag = match sfield_type.as_ref() {
                        "$ip" => ScanFlags::IP,
                        "$email" => ScanFlags::EMAIL,
                        "$date" => ScanFlags::DATE,
                        "$quoted" => ScanFlags::QUOTED,
                        "$url" => ScanFlags::URL,
                        _ => ScanFlags::NONE,
                    };
                    if scan_flags == ScanFlags::NONE {
                        scan_flags = flag;
                    } else {
                        scan_flags = scan_flags | flag;
                    }
                }
//                println!("flags: {:?}", scan_flags);
//
//                println!("Read all: {}", read_all);
//                println!("Positionals : {:?}", positional_fields);
//                println!("Smarts : {:?}", smart_fields);
//                println!("ordered  : {:?}", projections_ordered);

                // search across all datastores
                for ds in &cfg.datastore {
                    let msl_files = match list_msl_bucket_files(&log.name[..], ds) {
                        Ok(mf) => mf,
                        Err(e) => {
                            error!("Problem listing msl files {}", e);
                            let response = Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .header(header::CONTENT_TYPE, "text/plain")
                                .body(Body::from("fail"))?;
                            return Ok(response);
                        }
                    };
                    // for each file found inside the log
                    for f in msl_files {
                        // filter only files with msl extension
                        if f.contains(".msl") {
                            let lines = match read_file(&f, ds) {
                                Ok(l) => l,
                                Err(e) => {
                                    error!("problem reading file {}", e);
                                    let response = Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .header(header::CONTENT_TYPE, "text/plain")
                                        .body(Body::from("fail"))?;
                                    return Ok(response);
                                }
                            };
                            // process lines
                            let individual_lines = lines.split("\n");
                            for line in individual_lines {
                                let mut projection_values: HashMap<String, String> = HashMap::new();
                                // if we have position columns, process
                                if positional_fields.len() > 0 {
                                    //TODO: Use separator construct from header
                                    let parts: Vec<&str> = line.split(" ").collect();
                                    for pos in &positional_fields {
                                        if pos.position - 1 < (parts.len() as i32) {
                                            projection_values.insert(pos.alias.clone(), parts[(pos.position - 1) as usize].to_string());
                                        } else {
                                            projection_values.insert(pos.alias.clone(), "".to_string());
                                        }
                                    }
                                }
//                                let mut smart_values: HashMap<String, String> = HashMap::new();
                                if smart_fields.len() > 0 {
                                    let found_vals = scanlog(&line.to_string(), scan_flags);
                                    for smt in &smart_fields {
                                        if found_vals.contains_key(&smt.typed[..]) {
                                            // if the requested position is available
                                            if smt.position - 1 < (found_vals[&smt.typed].len() as i32) {
                                                projection_values.insert(smt.alias.clone(), found_vals[&smt.typed][(smt.position - 1) as usize].clone());
                                            } else {
                                                projection_values.insert(smt.alias.clone(), "".to_string());
                                            }
                                        }
                                    }
                                }

//                                println!("projection_values: {:?}", projection_values);

                                // filter the line
                                let mut skip_line = false;
                                match query {
                                    sqlparser::sqlast::SQLStatement::SQLSelect(ref q) => {
                                        match q.body {
                                            sqlparser::sqlast::SQLSetExpr::Select(ref bodyselect) => {
                                                let mut all_conditions_pass = true;
                                                for slct in &bodyselect.selection {
                                                    match slct {
                                                        sqlparser::sqlast::ASTNode::SQLIsNotNull(ast) => {
                                                            let identifier = match **ast {
                                                                sqlparser::sqlast::ASTNode::SQLIdentifier(ref identifier) => {
                                                                    identifier.to_string()
                                                                }
                                                                _ => {
                                                                    // TODO: Should we be retunring anything at all?
                                                                    "".to_string()
                                                                }
                                                            };
                                                            if projection_values.contains_key(&identifier[..]) == false || projection_values[&identifier] == "" {
                                                                all_conditions_pass = false;
                                                            }
                                                        }
                                                        sqlparser::sqlast::ASTNode::SQLIsNull(ast) => {
                                                            let identifier = match **ast {
                                                                sqlparser::sqlast::ASTNode::SQLIdentifier(ref identifier) => {
                                                                    identifier.to_string()
                                                                }
                                                                _ => {
                                                                    // TODO: Should we be retunring anything at all?
                                                                    "".to_string()
                                                                }
                                                            };
                                                            if projection_values[&identifier] != "" {
                                                                all_conditions_pass = false;
                                                            }
                                                        }
                                                        sqlparser::sqlast::ASTNode::SQLBinaryExpr { left, op, right } => {
                                                            let identifier = left.to_string();

                                                            match op {
                                                                sqlparser::sqlast::SQLOperator::Eq => {
                                                                    // TODO: Optimize this op_value preparation, don't do it in the loop
                                                                    let op_value = match **right {
                                                                        sqlparser::sqlast::ASTNode::SQLIdentifier(ref right_value) => {
                                                                            // Did they used double quotes for the value?
                                                                            let mut str_id = right_value.to_string();
                                                                            if str_id.starts_with("\"") {
                                                                                str_id = str_id[1..][..str_id.len() - 2].to_string();
                                                                            }
                                                                            str_id
                                                                        }
                                                                        sqlparser::sqlast::ASTNode::SQLValue(ref right_value) => {
                                                                            match right_value {
                                                                                sqlparser::sqlast::Value::SingleQuotedString(s) => { s.to_string() }
                                                                                _ => { right_value.to_string() }
                                                                            }
                                                                        }
                                                                        _ => {
                                                                            "".to_string()
                                                                        }
                                                                    };

                                                                    if projection_values.contains_key(&identifier[..]) && projection_values[&identifier] != op_value {
                                                                        all_conditions_pass = false;
                                                                    }
                                                                }
                                                                sqlparser::sqlast::SQLOperator::NotEq => {
                                                                    // TODO: Optimize this op_value preparation, don't do it in the loop
                                                                    let op_value = match **right {
                                                                        sqlparser::sqlast::ASTNode::SQLIdentifier(ref right_value) => {
                                                                            // Did they used double quotes for the value?
                                                                            let mut str_id = right_value.to_string();
                                                                            if str_id.starts_with("\"") {
                                                                                str_id = str_id[1..][..str_id.len() - 2].to_string();
                                                                            }
                                                                            str_id
                                                                        }
                                                                        sqlparser::sqlast::ASTNode::SQLValue(ref right_value) => {
                                                                            match right_value {
                                                                                sqlparser::sqlast::Value::SingleQuotedString(s) => { s.to_string() }
                                                                                _ => { right_value.to_string() }
                                                                            }
                                                                        }
                                                                        _ => {
                                                                            "".to_string()
                                                                        }
                                                                    };
                                                                    if projection_values.contains_key(&identifier[..]) && projection_values[&identifier] == op_value {
                                                                        all_conditions_pass = false;
                                                                    }
                                                                }
                                                                sqlparser::sqlast::SQLOperator::Like => {
                                                                    // TODO: Optimize this op_value preparation, don't do it in the loop
                                                                    let op_value = match **right {
                                                                        sqlparser::sqlast::ASTNode::SQLIdentifier(ref right_value) => {
                                                                            // Did they used double quotes for the value?
                                                                            let mut str_id = right_value.to_string();
                                                                            if str_id.starts_with("\"") {
                                                                                str_id = str_id[1..][..str_id.len() - 2].to_string();
                                                                            }
                                                                            str_id
                                                                        }
                                                                        sqlparser::sqlast::ASTNode::SQLValue(ref right_value) => {
                                                                            match right_value {
                                                                                sqlparser::sqlast::Value::SingleQuotedString(s) => { s.to_string() }
                                                                                _ => { right_value.to_string() }
                                                                            }
                                                                        }
                                                                        _ => {
                                                                            "".to_string()
                                                                        }
                                                                    };
                                                                    // TODO: Add support for wildcards ie: LIKE 'server_.domain.com' where _ is a single character wildcard
                                                                    if identifier == "$line" {
                                                                        if line.contains(&op_value[..]) == false {
                                                                            all_conditions_pass = false;
                                                                        }
                                                                    } else {
                                                                        if projection_values.contains_key(&identifier[..]) && projection_values[&identifier].contains(&op_value[..]) == false {
                                                                            all_conditions_pass = false;
                                                                        }
                                                                    }
                                                                }
                                                                _ => {
                                                                    info!("Unhandled operator");
                                                                }
                                                            }
                                                        }
                                                        _ => {
                                                            info!("Unhandled operation");
                                                        }
                                                    }
                                                }
                                                if all_conditions_pass == false {
                                                    skip_line = true;
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                    _ => {}
                                };

                                if skip_line == false {
                                    if read_all {
                                        resulting_data.push_str(line);
                                        resulting_data.push('\n');
                                    } else {
                                        // build the result
                                        // iterate over the ordered resulting projections
                                        let mut field_values: Vec<String> = Vec::new();
                                        for proj in &projections_ordered {
                                            // check if it's in positionsals
                                            if projection_values.contains_key(proj) {
                                                field_values.push(projection_values[proj].clone());
                                            }
                                        }
                                        // TODO: When adding CSV output, change the separator
                                        resulting_data.push_str(&field_values.join(" ")[..]);
                                        resulting_data.push('\n');
                                    }
                                }
                            }
                        }
                    }
                }
            }
            let duration = start.elapsed();
            info!("Search took: {:?}", duration);
            let response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from(resulting_data))?;
            Ok(response)
        })
    )
}

