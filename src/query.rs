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
use std::collections::HashSet;
use std::error;
use std::fmt;
use std::time::Instant;

use futures::future::FutureResult;
use futures::Sink;
use futures::{stream, Future, Stream};
use hyper::{header, Body, Chunk, Request, Response, StatusCode};
use regex::Regex;
use sqlparser::sqlast::SQLStatement;
use sqlparser::sqlparser::Parser;
use sqlparser::sqlparser::ParserError;
use tokio::sync::mpsc;

use crate::config::Config;
use crate::constants::SF_DATE;
use crate::constants::SF_EMAIL;
use crate::constants::SF_IP;
use crate::constants::SF_QUOTED;
use crate::constants::SF_URL;
use crate::dialect::MinSQLDialect;
use crate::http::GenericError;
use crate::http::ResponseFuture;
use crate::storage::list_msl_bucket_files;
use crate::storage::read_file;

bitflags! {
    // ScanFlags determine which regex should be evaluated
    // If you are adding new values make sure to add the next power of 2 as
    // they are evaluated using a bitwise operation
    pub struct ScanFlags: u32 {
        const IP = 1;
        const EMAIL = 2;
        const DATE = 4;
        const QUOTED = 8;
        const URL = 16;
        const NONE = 32;
    }
}

#[derive(Debug, Clone)]
struct PositionalColumn {
    position: i32,
    alias: String,
}

#[derive(Debug, Clone)]
struct SmartColumn {
    // $ip, $email...
    typed: String,
    // for $ip or $ip1 is 1, for $ip2 is 2 ...
    position: i32,
    // if this column was aliased
    alias: String,
}

#[derive(Debug)]
pub struct ParseSqlError;

impl fmt::Display for ParseSqlError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error parsing sql")
    }
}

impl error::Error for ParseSqlError {
    fn description(&self) -> &str {
        "Error parsing sql"
    }

    fn cause(&self) -> Option<&error::Error> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

pub fn parse_query(entire_body: Chunk) -> FutureResult<Vec<SQLStatement>, GenericError> {
    let payload: String = match String::from_utf8(entire_body.to_vec()) {
        Ok(str) => str,
        Err(err) => panic!("Couldn't convert buffer to string: {}", err),
    };

    // attempt to parse the payload
    let dialect = MinSQLDialect {};
    //    let ast = Parser::parse_sql(&dialect, payload.clone());

    //    futures::future::result(ast)
    match Parser::parse_sql(&dialect, payload.clone()) {
        Ok(q) => futures::future::ok(q),
        Err(e) => {
            // Unable to parse query, match reason
            match e {
                ParserError::TokenizerError(s) => {
                    error!("Failed to tokenize query `{}`: {}", payload.clone(), s);
                }
                ParserError::ParserError(s) => {
                    error!("Failed to parse query `{}`: {}", payload.clone(), s);
                }
            }
            // TODO: Design a more informative error message
            futures::future::err::<Vec<SQLStatement>, GenericError>(ParseSqlError.into())
        }
    }
}

pub fn validate_logs(
    cfg: &Config,
    ast: Vec<SQLStatement>,
) -> FutureResult<Vec<SQLStatement>, GenericError> {
    // Validate all the tables for all the queries, we don't want to start serving content
    // for the first query and then discover subsequent queries are invalid
    for query in &ast {
        // find the table they want to query
        let some_table = match query {
            sqlparser::sqlast::SQLStatement::SQLSelect(ref q) => match q.body {
                sqlparser::sqlast::SQLSetExpr::Select(ref bodyselect) => {
                    bodyselect.relation.clone()
                }
                _ => None,
            },
            _ => {
                error!("Not the type of query we support");
                None
            }
        };
        if some_table == None {
            error!("No table found");
            return futures::future::err::<Vec<SQLStatement>, GenericError>(ParseSqlError.into());
        }
        let table = some_table.unwrap().to_string();
        let loggy = cfg.get_log(&table);
        if loggy.is_none() {
            return futures::future::err::<Vec<SQLStatement>, GenericError>(ParseSqlError.into());
        }
    }

    futures::future::ok(ast)
}

pub fn scanlog(text: &String, flags: ScanFlags) -> HashMap<String, Vec<String>> {
    // Compile the regex only once
    lazy_static! {
        static ref IP_RE :Regex= Regex::new(r"(((25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9][0-9]|[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9][0-9]|[0-9]))").unwrap();
        static ref EMAIL_RE :Regex= Regex::new(r"([\w\.!#$%&'*+\-=?\^_`{|}~]+@([\w\d-]+\.)+[\w]{2,4})").unwrap();
        // TODO: This regex matches a fairly simple date format, improve : 2019-05-23
        static ref DATE_RE :Regex= Regex::new(r"((19[789]\d|2\d{3})[-/](0[1-9]|1[1-2])[-/](0[1-9]|[1-2][0-9]|3[0-1]*))|((0[1-9]|[1-2][0-9]|3[0-1]*)[-/](Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|(0[1-9]|1[1-2]))[-/](19[789]\d|2\d{3}))").unwrap();
        static ref QUOTED_RE :Regex= Regex::new("((\"(.*?)\")|'(.*?)')").unwrap();
        static ref URL_RE :Regex= Regex::new(r#"(https?|ftp)://[^\s/$.?#].[^()\]\[\s]*"#).unwrap();
    }
    let mut results: HashMap<String, Vec<String>> = HashMap::new();

    if flags.contains(ScanFlags::IP) {
        let mut items: Vec<String> = Vec::new();
        for cap in IP_RE.captures_iter(text) {
            items.push(cap[0].to_string())
        }
        results.insert(SF_IP.to_string(), items);
    }
    if flags.contains(ScanFlags::EMAIL) {
        let mut items: Vec<String> = Vec::new();
        for cap in EMAIL_RE.captures_iter(text) {
            items.push(cap[0].to_string())
        }
        results.insert(SF_EMAIL.to_string(), items);
    }
    if flags.contains(ScanFlags::DATE) {
        let mut items: Vec<String> = Vec::new();
        for cap in DATE_RE.captures_iter(text) {
            items.push(cap[0].to_string())
        }
        results.insert(SF_DATE.to_string(), items);
    }
    if flags.contains(ScanFlags::QUOTED) {
        let mut items: Vec<String> = Vec::new();
        for cap in QUOTED_RE.captures_iter(text) {
            items.push(cap[0].to_string())
        }
        results.insert(SF_QUOTED.to_string(), items);
    }
    if flags.contains(ScanFlags::URL) {
        let mut items: Vec<String> = Vec::new();
        for cap in URL_RE.captures_iter(text) {
            items.push(cap[0].to_string())
        }
        results.insert(SF_URL.to_string(), items);
    }
    results
}

#[derive(Debug, Clone)]
struct QueryParsing {
    read_all: bool,
    scan_flags: ScanFlags,
    positional_fields: Vec<PositionalColumn>,
    smart_fields: Vec<SmartColumn>,
    projections_ordered: Vec<String>,
}

// performs a query on a log
pub fn api_log_search(cfg: &'static Config, req: Request<Body>) -> ResponseFuture {
    let start = Instant::now();
    lazy_static! {
        static ref SMART_FIELDS_RE: Regex =
            Regex::new(r"((\$(ip|email|date|url|quoted))([0-9]+)*)\b").unwrap();
    };

    // A web api to run against
    Box::new(req.into_body()
        .concat2() // Concatenate all chunks in the body
        .from_err()
        .and_then(parse_query)
        .map_err(|e| {
            error!("----{}", e);
            e
        })
        .and_then(move |ast| validate_logs(&cfg, ast))
        .map_err(|e| {
            error!("----{}", e);
            e
        })
        .and_then(move |ast| {
            let mut queries_parse: HashMap<String, QueryParsing> = HashMap::new();

            // We are going to validate the whole payload.
            // Make sure tables are valid, projections are valid and filtering operations are supported
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

                // we keep track of the parsing of the queries in order
                let query_string = query.to_string();
                queries_parse.insert(query_string, QueryParsing {
                    read_all,
                    scan_flags,
                    positional_fields,
                    smart_fields,
                    projections_ordered,
                });
            }
            // if we reach this point, no query was invalid

            let (tx, rx) = mpsc::unbounded_channel();
            let (body_tx, body) = hyper::Body::channel();

            let ast = ast.clone();
            let cfg = cfg.clone();
            let queries_parse = queries_parse.clone();

            // producer task
            hyper::rt::spawn({
                // for each query, retrive data
                stream::iter_ok(ast).fold(tx, move |tx, query| {
                    // find the table they want to query
                    let some_table = match query {
                        sqlparser::sqlast::SQLStatement::SQLSelect(ref q) => {
                            match q.body {
                                sqlparser::sqlast::SQLSetExpr::Select(ref bodyselect) => {
                                    bodyselect.relation.clone()
                                }
                                _ => {
                                    // this shouldn't happen
                                    None
                                }
                            }
                        }
                        _ => {
                            None
                        }
                    };

                    let table = some_table.unwrap().to_string();
                    // should be safe to unwrap since query validation made sure the log exists
                    let log = cfg.get_log(&table).unwrap();

                    let log = log.clone();
                    let my_ds = cfg.datastore.clone();
                    let queries_parse = queries_parse.clone();
                    let query = query.clone();
                    stream::iter_ok(my_ds).fold(tx, move |tx, ds| {
                        let msl_files = match list_msl_bucket_files(&log.name[..], &ds) {
                            Ok(mf) => mf,
                            Err(e) => {
                                // TODO: Handler Error. Ideally, we should end the channel and terminate the stream
                                // use take_while http://xion.io/post/code/rust-stream-terminate.html with Result
                                error!("error reading files from minIO {:?}", e);
                                Vec::new()
                            }
                        };
                        // for each file found inside the log
                        let ds = ds.clone();
                        let queries_parse = queries_parse.clone();
                        let query = query.clone();
                        stream::iter_ok(msl_files).fold(tx, move |tx, f| {
                            // TODO: Ideally we want to work with the streaming body
                            let all_file_lines = match read_file(&f, &ds) {
                                Ok(l) => l,
                                Err(e) => {
                                    error!("problem reading file {}", e);
                                    // TODO: Handle error. Ideally, we want to stop the channel
                                    "".to_string()
                                }
                            };


                            let individual_lines: Vec<String> = all_file_lines.lines().map(|x| x.to_string()).collect();
                            let queries_parse = queries_parse.clone();
                            let query = query.clone();
                            stream::iter_ok(individual_lines).fold(tx, move |tx, line| {
                                let mut projection_values: HashMap<String, String> = HashMap::new();
                                let query_data = queries_parse.get(&query.to_string()[..]).unwrap();
                                // if we have position columns, process
                                if query_data.positional_fields.len() > 0 {
                                    // TODO: Use separator construct from header
                                    let parts: Vec<&str> = line.split(" ").collect();
                                    for pos in &query_data.positional_fields {
                                        if pos.position - 1 < (parts.len() as i32) {
                                            projection_values.insert(pos.alias.clone(), parts[(pos.position - 1) as usize].to_string());
                                        } else {
                                            projection_values.insert(pos.alias.clone(), "".to_string());
                                        }
                                    }
                                }

                                if query_data.smart_fields.len() > 0 {
                                    let found_vals = scanlog(&line.to_string(), query_data.scan_flags);
                                    for smt in &query_data.smart_fields {
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

                                let mut outline: String = String::new();

                                if skip_line == false {
                                    if query_data.read_all {
                                        outline = line.clone();
                                        outline.push('\n');
                                    } else {
                                        // build the result
                                        // iterate over the ordered resulting projections
                                        let mut field_values: Vec<String> = Vec::new();
                                        for proj in &query_data.projections_ordered {
                                            // check if it's in positionsals
                                            if projection_values.contains_key(proj) {
                                                field_values.push(projection_values[proj].clone());
                                            }
                                        }
                                        // TODO: When adding CSV output, change the separator
                                        outline = field_values.join(" ");
                                        outline.push('\n');
                                    }
                                }
                                tx.send(outline).map_err(|e| println!("{:?}", e))
                            })
                        })
                    })
                }).map(|_| ()) // Drop tx handle
            });

            // consumer task
            hyper::rt::spawn({
                rx
                    .map_err(|e| {
                        println!("{:?}", e);
                    })
                    .fold(body_tx, move |body_tx, msg| {
                        body_tx.send(Chunk::from(msg))
                            .map_err(|e| println!("error = {:?}", e))
                    })
                    .map(|_| ()) // drop body_tx handle
            });


            let duration = start.elapsed();
            info!("Search took: {:?}", duration);
            Ok(Response::new(body))
        })
    )
}
