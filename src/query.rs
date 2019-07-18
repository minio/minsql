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
use std::error::Error;
use std::fmt;
use std::sync::{Arc, RwLock};

use futures::sink::Sink;
use futures::{stream, Future, Stream};
use hyper::{Body, Chunk, Request, Response};
use log::{error, info};
use regex::Regex;
use sqlparser::ast::{BinaryOperator, Expr, SelectItem, SetExpr, Statement, Value};
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use tokio::sync::mpsc;

use bitflags::bitflags;
use lazy_static::lazy_static;

use crate::auth::Auth;
use crate::combinators::take_from_iterable::TakeFromIterable;
use crate::config::Config;
use crate::constants::SF_DATE;
use crate::constants::SF_EMAIL;
use crate::constants::SF_IP;
use crate::constants::SF_QUOTED;
use crate::constants::SF_URL;
use crate::dialect::MinSQLDialect;
use crate::filter::line_fails_query_conditions;
use crate::http::GenericError;
use crate::http::ResponseFuture;
use crate::http::{return_400, return_401};
use crate::storage::{list_msl_bucket_files, read_file_line_by_line};

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

#[derive(Debug, Clone, PartialEq)]
struct PositionalColumn {
    position: i32,
    alias: String,
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug)]
pub enum QueryError {
    Underlying(String),
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for QueryError {
    fn description(&self) -> &str {
        "query error?"
    }
}

pub struct Query {
    config: Arc<RwLock<Config>>,
}

impl Query {
    pub fn new(cfg: Arc<RwLock<Config>>) -> Query {
        Query { config: cfg }
    }

    pub fn parse_query(&self, payload: String) -> Result<Vec<Statement>, GenericError> {
        // attempt to parse the payload
        let dialect = MinSQLDialect {};

        match Parser::parse_sql(&dialect, payload.clone()) {
            Ok(q) => Ok(q),
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
                Err(ParseSqlError.into())
            }
        }
    }

    pub fn validate_logs(&self, ast: &Vec<Statement>) -> Option<GenericError> {
        let cfg = self.config.read().unwrap();
        // Validate all the tables for all the  queries, we don't want to start serving content
        // for the first query and then discover subsequent queries are invalid
        for query in ast {
            // find the table they want to query
            let some_table = match query {
                Statement::Query(q) => match q.body {
                    // TODO: Validate a single table
                    SetExpr::Select(ref bodyselect) => Some(bodyselect.from[0].relation.clone()),
                    _ => None,
                },
                _ => {
                    error!("Not the type of query we support");
                    None
                }
            };
            if some_table == None {
                error!("No table found");
                return Some(ParseSqlError.into());
            }
            let table = some_table.unwrap().to_string();
            let loggy = cfg.get_log(&table);
            if loggy.is_none() {
                return Some(ParseSqlError.into());
            }
        }
        None
    }

    // performs a query on a log
    pub fn api_log_search(&self, req: Request<Body>, access_token: &String) -> ResponseFuture {
        let access_token = access_token.clone();
        let cfg = Arc::clone(&self.config);
        let query_c = Query::new(cfg);

        let query_state_holder = Arc::new(RwLock::new(StateHolder::new()));
        let query_state_holder = Arc::clone(&query_state_holder);
        // A web api to run against
        Box::new(
            req.into_body()
                .concat2() // Concatenate all chunks in the body
                .from_err()
                .and_then(move |entire_body| {
                    let payload: String = match String::from_utf8(entire_body.to_vec()) {
                        Ok(str) => str,
                        Err(_) => {
                            return Ok(return_400("Could not understand request"));
                        }
                    };
                    let ast = match query_c.parse_query(payload) {
                        Ok(v) => v,
                        Err(e) => {
                            return Ok(return_400(format!("{:?}", e).as_str()));
                        }
                    };
                    if let Some(_) = query_c.validate_logs(&ast) {
                        return Ok(return_400("invalid log name"));
                    };

                    // Translate the SQL AST into a `QueryParsing`
                    // that has all the elements needed to continue
                    let parsed_queries = match query_c.process_sql(&access_token, ast) {
                        Ok(v) => v,
                        Err(e) => {
                            return match e {
                                ProcessingQueryError::Fail(s) => Ok(return_400(s.clone().as_str())),
                                ProcessingQueryError::UnsupportedQuery(s) => {
                                    Ok(return_400(s.clone().as_str()))
                                }
                                ProcessingQueryError::NoTableFound(s) => {
                                    Ok(return_400(s.clone().as_str()))
                                }
                                ProcessingQueryError::Unauthorized(_s) => Ok(return_401()),
                            };
                        }
                    };
                    let total_querys = parsed_queries.len();
                    let mut writable_state = query_state_holder.write().unwrap();
                    writable_state.query_parsing = parsed_queries;

                    // prepare copies to go into the next future

                    let cfg = Arc::clone(&query_c.config);

                    let query_state_holder = Arc::clone(&query_state_holder);

                    let body_str = stream::iter_ok::<_, QueryError>(0..total_querys)
                        .map(move |query_index| {
                            // for each query parse, read from all datasources for the log
                            let read_state_holder = query_state_holder.read().unwrap();
                            let q_parse = &read_state_holder.query_parsing[query_index].1;
                            let cfg_read = cfg.read().unwrap();
                            let log = cfg_read.get_log(&q_parse.log_name).unwrap();
                            let log_datastores = &log.datastores;

                            let limit = q_parse.limit.unwrap_or(std::u64::MAX);

                            let logs_ds_len = log_datastores.len();

                            // prepare copies to go into the next future
                            let cfg = Arc::clone(&cfg);
                            let query_state_holder = Arc::clone(&query_state_holder);
                            let query_state_holder3 = Arc::clone(&query_state_holder);

                            let (tx, rx) = mpsc::unbounded_channel::<Vec<String>>();
                            // For each datastore in the log we are going to spawn a task to read the
                            // logs stored in given datastore.
                            for i in 0..logs_ds_len {
                                let cfg2 = Arc::clone(&cfg);
                                let query_state_holder2 = Arc::clone(&query_state_holder);
                                let tx = tx.clone();
                                // Task that will read all the logs for a given datastore
                                let task = stream::iter_ok(i..i + 1)
                                    .map(move |log_ds_index| {
                                        let cfg2 = Arc::clone(&cfg2);
                                        let query_state_holder2 = Arc::clone(&query_state_holder2);
                                        // let log_ds_index = log_ds_index.clone();
                                        Query::read_logs_from_datastore(
                                            cfg2,
                                            query_state_holder2,
                                            query_index,
                                            log_ds_index,
                                        )
                                    })
                                    .flatten()
                                    .fold(tx, |tx, lines| {
                                        tx.send(lines)
                                            .map_err(|e| QueryError::Underlying(format!("{:?}", e)))
                                    })
                                    .map_err(|_| ())
                                    .map(|_| ());
                                tokio::spawn(task);
                            }

                            rx.map_err(|e| QueryError::Underlying(format!("{:?}", e))) //temporarely remove error, we need to adress this
                                .map(move |lines| {
                                    let query_state_holder4 = Arc::clone(&query_state_holder3);
                                    let read_state_holder = query_state_holder4.read().unwrap();
                                    let q = &read_state_holder.query_parsing[query_index].0;
                                    let q_parse = &read_state_holder.query_parsing[query_index].1;
                                    lines
                                        .into_iter()
                                        .filter_map(|line| {
                                            evaluate_query_on_line(&q, &q_parse, line)
                                            //                                                Some(line)
                                        })
                                        .collect::<Vec<String>>()
                                })
                                .take_from_iterable(limit)
                        })
                        .flatten()
                        .map(|s: Vec<String>| Chunk::from(s.join("\n") + &"\n"));
                    Ok(Response::new(Body::wrap_stream(body_str)))
                }),
        )
    }

    fn process_statement(
        &self,
        access_token: &String,
        query: Statement,
    ) -> Result<(Statement, QueryParsing), ProcessingQueryError> {
        lazy_static! {
            static ref SMART_FIELDS_RE: Regex =
                Regex::new(r"((\$(ip|email|date|url|quoted))([0-9]+)*)\b").unwrap();
        };
        // find the table they want to query
        let some_table = match query {
            Statement::Query(ref q) => {
                match q.body {
                    SetExpr::Select(ref bodyselect) => {
                        // TODO: Validate a single table
                        Some(bodyselect.from[0].relation.clone())
                    }
                    _ => {
                        return Err(ProcessingQueryError::Fail("No Table Found".to_string()));
                    }
                }
            }
            _ => {
                return Err(ProcessingQueryError::UnsupportedQuery(
                    "Unsupported query".to_string(),
                ));
            }
        };
        if some_table == None {
            return Err(ProcessingQueryError::NoTableFound(
                "No table was found in the query statement".to_string(),
            ));
        }
        let log_name = some_table.unwrap().to_string().clone();

        // check if we have access for the requested table
        let cfg = Arc::clone(&self.config);
        let auth_c = Auth::new(cfg);
        if !auth_c.token_has_access_to_log(&access_token[..], &log_name[..]) {
            return Err(ProcessingQueryError::Unauthorized(
                "Unauthorized".to_string(),
            ));
        }

        // determine our read strategy
        let read_all = match query {
            Statement::Query(ref q) => match q.body {
                SetExpr::Select(ref bodyselect) => {
                    let mut is_wildcard = false;
                    for projection in &bodyselect.projection {
                        if *projection == SelectItem::Wildcard {
                            is_wildcard = true
                        }
                    }
                    is_wildcard
                }
                _ => false,
            },
            _ => false,
        };

        let projections = match query {
            Statement::Query(ref q) => {
                match q.body {
                    SetExpr::Select(ref bodyselect) => bodyselect.projection.clone(),
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
                SelectItem::UnnamedExpr(ref ast) => {
                    // we have an identifier
                    match ast {
                        Expr::Identifier(ref identifier) => {
                            let id_name = &identifier[1..];
                            let position = match id_name.parse::<i32>() {
                                Ok(p) => p,
                                Err(_) => -1,
                            };
                            // if we were able to parse identifier as an i32 it's a positional
                            if position > 0 {
                                positional_fields.push(PositionalColumn {
                                    position: position,
                                    alias: identifier.clone(),
                                });
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
                                    smart_fields.push(SmartColumn {
                                        typed: typed.clone(),
                                        position: pos,
                                        alias: identifier.clone(),
                                    });
                                    // record the order or extraction
                                    projections_ordered.push(identifier.clone());
                                }
                            }
                        }
                        _ => (),
                    }
                }
                _ => {} // for now let's not do anything on other Variances
            }
        }

        // see which fields in the conditions were not requested in the projections and extract them too
        let limit = match query {
            Statement::Query(ref q) => {
                match q.body {
                    SetExpr::Select(ref bodyselect) => {
                        for slct in &bodyselect.selection {
                            process_fields_for_ast(
                                slct,
                                &mut positional_fields,
                                &mut smart_fields,
                                &mut smart_fields_set,
                            );
                        }
                    }
                    _ => {}
                }
                match &q.limit {
                    Some(limit_node) => match limit_node {
                        Expr::Value(val) => match val {
                            Value::Long(l) => Some(l.clone()),
                            _ => None,
                        },
                        _ => None,
                    },
                    None => None,
                }
            }
            _ => None,
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

        // we keep track of the parsing of the queries via their signature.
        Ok((
            query,
            QueryParsing {
                log_name,
                read_all,
                scan_flags,
                positional_fields,
                smart_fields,
                projections_ordered,
                limit,
            },
        ))
    }

    /// Parses a vector sql statements and returns a parsed summary
    /// structure for each.
    pub fn process_sql(
        &self,
        access_token: &String,
        ast: Vec<Statement>,
    ) -> Result<Vec<(Statement, QueryParsing)>, ProcessingQueryError> {
        ast.into_iter()
            .map(|q| self.process_statement(&access_token, q))
            .collect()
    }

    /// Reads all the log files for a given `QueryParse` in marked `DataSource`
    fn read_logs_from_datastore(
        cfg: Arc<RwLock<Config>>,
        query_state_holder: Arc<RwLock<StateHolder>>,
        query_index: usize,
        log_ds_index: usize,
    ) -> impl Stream<Item = Vec<String>, Error = QueryError> {
        let cfg_read = cfg.read().unwrap();
        let read_state_holder = query_state_holder.read().unwrap();

        // Get the `QueryParse` and the `Log` from the indexes provided
        let q_parse = &read_state_holder.query_parsing[query_index].1;
        let log = cfg_read.get_log(&q_parse.log_name).unwrap();

        let ds_name = &log.datastores[log_ds_index];

        let log_name = cfg
            .read()
            .unwrap()
            .get_log(&q_parse.log_name)
            .unwrap()
            .name
            .clone()
            .unwrap();
        // validation should make this unwrapping safe
        let ds = cfg_read.datastore.get(ds_name.as_str()).unwrap();
        let cfg2 = Arc::clone(&cfg);
        let query_state_holder2 = Arc::clone(&query_state_holder);
        // Returns Result<(ds, files), error>. Need to stop on error.
        // TODO: Stop on error
        list_msl_bucket_files(log_name.as_str(), &ds)
            .map(move |obj_key| (query_index.clone(), log_ds_index.clone(), obj_key))
            .map_err(|e| QueryError::Underlying(format!("{:?}", e))) //temporarely remove error, we need to adress this
            .map(move |(query_index, log_ds_index, obj_key)| {
                let read_state_holder = query_state_holder2.read().unwrap();
                let q_parse = &read_state_holder.query_parsing[query_index].1;

                let cfg_read = cfg2.read().unwrap();
                let log = cfg_read.get_log(&q_parse.log_name).unwrap();

                let ds_name = &log.datastores[log_ds_index];
                let ds = cfg_read.datastore.get(ds_name).unwrap();

                read_file_line_by_line(&obj_key, &ds)
                    .map_err(|e| QueryError::Underlying(format!("{:?}", e)))
            })
            .flatten()
    }
}

fn process_fields_for_ast(
    ast_node: &Expr,
    positional_fields: &mut Vec<PositionalColumn>,
    smart_fields: &mut Vec<SmartColumn>,
    smart_fields_set: &mut HashSet<String>,
) {
    lazy_static! {
        static ref SMART_FIELDS_RE: Regex =
            Regex::new(r"((\$(ip|email|date|url|quoted))([0-9]+)*)\b").unwrap();
    };
    match ast_node {
        Expr::Nested(nested_ast) => {
            process_fields_for_ast(
                nested_ast,
                positional_fields,
                smart_fields,
                smart_fields_set,
            );
        }
        Expr::IsNotNull(ast) => {
            let identifier = match **ast {
                Expr::Identifier(ref identifier) => identifier.to_string(),
                _ => {
                    // TODO: Should we be retunring anything at all?
                    "".to_string()
                }
            };
            //positional or smart?
            let id_name = &identifier[1..];
            let position = match id_name.parse::<i32>() {
                Ok(p) => p,
                Err(_) => -1,
            };
            // if we were able to parse identifier as an i32 it's a positional
            if position > 0 {
                positional_fields.push(PositionalColumn {
                    position: position,
                    alias: identifier.clone(),
                });
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
                    smart_fields.push(SmartColumn {
                        typed: typed.clone(),
                        position: pos,
                        alias: identifier.clone(),
                    });
                }
            }
        }
        Expr::IsNull(ast) => {
            let identifier = match **ast {
                Expr::Identifier(ref identifier) => identifier.to_string(),
                _ => {
                    // TODO: Should we be retunring anything at all?
                    "".to_string()
                }
            };
            //positional or smart?
            let id_name = &identifier[1..];
            let position = match id_name.parse::<i32>() {
                Ok(p) => p,
                Err(_) => -1,
            };
            // if we were able to parse identifier as an i32 it's a positional
            if position > 0 {
                positional_fields.push(PositionalColumn {
                    position: position,
                    alias: identifier.clone(),
                });
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
                    smart_fields.push(SmartColumn {
                        typed: typed.clone(),
                        position: pos,
                        alias: identifier.clone(),
                    });
                }
            }
        }
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::And => {
                    process_fields_for_ast(left, positional_fields, smart_fields, smart_fields_set);
                    process_fields_for_ast(
                        right,
                        positional_fields,
                        smart_fields,
                        smart_fields_set,
                    );
                }
                BinaryOperator::Or => {
                    process_fields_for_ast(left, positional_fields, smart_fields, smart_fields_set);
                    process_fields_for_ast(
                        right,
                        positional_fields,
                        smart_fields,
                        smart_fields_set,
                    );
                }
                _ => {
                    let identifier = left.to_string();

                    //positional or smart?
                    let id_name = &identifier[1..];
                    let position = match id_name.parse::<i32>() {
                        Ok(p) => p,
                        Err(_) => -1,
                    };
                    // if we were able to parse identifier as an i32 it's a positional
                    if position > 0 {
                        positional_fields.push(PositionalColumn {
                            position: position,
                            alias: identifier.clone(),
                        });
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
                            smart_fields.push(SmartColumn {
                                typed: typed.clone(),
                                position: pos,
                                alias: identifier.clone(),
                            });
                        }
                    }
                }
            }
        }
        _ => {
            info!("Unhandled operation");
        }
    }
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
            // Validate whether we matched group 3 or 4 fromt he regex
            if cap.get(3) != None {
                items.push(cap[3].to_string())
            } else {
                items.push(cap[4].to_string())
            }
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

pub fn extract_positional_fields(
    projection_values: &mut HashMap<String, Option<String>>,
    query_data: &QueryParsing,
    line: &String,
) {
    if query_data.positional_fields.len() > 0 {
        // TODO: Use separator construct from header
        let parts: Vec<&str> = line.split(" ").collect();
        for pos in &query_data.positional_fields {
            let key = pos.alias.clone();
            if pos.position - 1 < (parts.len() as i32) {
                projection_values.insert(key, Some(parts[(pos.position - 1) as usize].to_string()));
            } else {
                projection_values.insert(key, None);
            }
        }
    }
}

pub fn extract_smart_fields(
    projection_values: &mut HashMap<String, Option<String>>,
    query_data: &QueryParsing,
    line: &String,
) {
    if query_data.smart_fields.len() > 0 {
        let found_vals = scanlog(line, query_data.scan_flags);
        for smt in &query_data.smart_fields {
            if found_vals.contains_key(&smt.typed[..]) {
                // if the requested position is available
                let key = smt.alias.clone();
                if smt.position - 1 < (found_vals[&smt.typed].len() as i32) {
                    projection_values.insert(
                        key,
                        Some(found_vals[&smt.typed][(smt.position - 1) as usize].clone()),
                    );
                } else {
                    projection_values.insert(key, None);
                }
            }
        }
    }
}

fn mk_output_line(
    projection_values: HashMap<String, Option<String>>,
    query_data: &QueryParsing,
    line: String,
) -> Option<String> {
    if query_data.read_all {
        Some(line)
    } else {
        // build the result iterate over the ordered resulting
        // projections
        let mut field_values: Vec<&Option<String>> = Vec::new();
        for i in 0..query_data.projections_ordered.len() {
            let proj = &query_data.projections_ordered[i];
            if projection_values.contains_key(proj) {
                let val = projection_values.get(proj).unwrap();
                field_values.push(&val);
            }
        }

        // Avoid cloning projection values, project `None` as ""
        let mut outstring = "".to_string();
        for i in 0..field_values.len() {
            let s = field_values[i];
            match s {
                Some(sv) => {
                    outstring.push_str(sv);
                }
                None => {
                    outstring.push_str("");
                }
            }

            if i + 1 < field_values.len() {
                outstring.push_str(" ");
            }
        }
        // TODO: When adding CSV output, change the separator
        Some(outstring)
    }
}

fn evaluate_query_on_line(
    query: &Statement,
    query_data: &QueryParsing,
    line: String,
) -> Option<String> {
    let mut projection_values: HashMap<String, Option<String>> = HashMap::new();

    // Extract projections
    extract_positional_fields(&mut projection_values, query_data, &line);
    extract_smart_fields(&mut projection_values, query_data, &line);

    // we can skip the line all together if we gonna project an empty line
    if query_data.read_all == false {
        let mut total_nones = 0;
        for i in 0..query_data.projections_ordered.len() {
            let proj = &query_data.projections_ordered[i];
            if projection_values.contains_key(proj) {
                let val = projection_values.get(proj).unwrap();
                if val.is_none() {
                    total_nones = total_nones + 1;
                }
            } else {
                // we don't even have the requested projection in the found projections
                total_nones = total_nones + 1;
            }
        }
        if total_nones == query_data.projections_ordered.len() {
            return None;
        }
    }

    // filter the line
    let skip_line = line_fails_query_conditions(&line, &query, &projection_values);
    if !skip_line {
        mk_output_line(projection_values, query_data, line)
    } else {
        None
    }
}

/// This struct represents the reading and filtering parameters that MinSQL uses to filter and
/// format the returned data.
#[derive(Debug, Clone)]
pub struct QueryParsing {
    log_name: String,
    read_all: bool,
    scan_flags: ScanFlags,
    positional_fields: Vec<PositionalColumn>,
    smart_fields: Vec<SmartColumn>,
    projections_ordered: Vec<String>,
    limit: Option<u64>,
}

#[derive(Debug)]
pub enum ProcessingQueryError {
    Fail(String),
    UnsupportedQuery(String),
    NoTableFound(String),
    Unauthorized(String),
}

struct StateHolder {
    query_parsing: Vec<(Statement, QueryParsing)>,
}

impl StateHolder {
    fn new() -> StateHolder {
        StateHolder {
            query_parsing: Vec::new(),
        }
    }
}

#[cfg(test)]
mod query_tests {
    use crate::config::{Config, Log, LogAuth, Server, Token};

    use super::*;

    static VALID_TOKEN: &str = "TOKEN1TOKEN1TOKEN1TOKEN1TOKEN1TOKEN1TOKEN1TOKEN1";
    static VALID_TOKEN2: &str = "TOKEN2TOKEN2TOKEN2TOKEN2TOKEN2TOKEN2TOKEN2TOKEN2";

    // Generates a Config object with only one auth item for one log
    fn get_ds_log_auth_config_for(log_name: String, token: &String) -> Config {
        let mut log_map = HashMap::new();
        log_map.insert(
            log_name.clone(),
            Log {
                name: Some(log_name.clone()),
                datastores: Vec::new(),
                commit_window: "5s".to_string(),
            },
        );

        let mut log_auth_map: HashMap<String, LogAuth> = HashMap::new();
        log_auth_map.insert(
            log_name.clone(),
            LogAuth {
                log_name: log_name,
                api: Vec::new(),
                expire: "".to_string(),
                status: "".to_string(),
            },
        );

        let mut auth = HashMap::new();
        auth.insert(token[0..16].to_string(), log_auth_map);

        let mut tokens = HashMap::new();
        tokens.insert(
            token[0..16].to_string(),
            Token {
                access_key: token[0..16].to_string(),
                secret_key: token[16..48].to_string(),
                description: None,
                is_admin: false,
                enabled: true,
            },
        );

        let cfg = Config {
            server: Server {
                address: "".to_string(),
                metadata_endpoint: "".to_string(),
                metadata_bucket: "".to_string(),
                access_key: "".to_string(),
                secret_key: "".to_string(),
                pkcs12_cert: None,
                pkcs12_password: None,
            },
            datastore: HashMap::new(),
            tokens: tokens,
            log: log_map,
            auth: auth,
        };
        cfg
    }

    #[test]
    fn process_simple_select() {
        let access_token = VALID_TOKEN.to_string();

        let cfg = get_ds_log_auth_config_for("mylog".to_string(), &access_token);
        let cfg = Arc::new(RwLock::new(cfg));
        let query_c = Query::new(cfg);

        let query = "SELECT * FROM mylog".to_string();
        let ast = query_c.parse_query(query).unwrap();
        let queries_parse = query_c.process_sql(&access_token, ast);

        match queries_parse {
            Ok(pq) => {
                let mqp = &pq[0].1;
                assert_eq!(mqp.log_name, "mylog");
                assert_eq!(mqp.read_all, true);
            }
            _ => panic!("error"),
        }
    }

    #[test]
    fn process_simple_select_limit() {
        let access_token = VALID_TOKEN.to_string();

        let cfg = get_ds_log_auth_config_for("mylog".to_string(), &access_token);
        let cfg = Arc::new(RwLock::new(cfg));
        let query_c = Query::new(cfg);

        let query = "SELECT * FROM mylog LIMIT 10".to_string();
        let ast = query_c.parse_query(query.clone()).unwrap();
        let queries_parse = query_c.process_sql(&access_token, ast);

        match queries_parse {
            Ok(pq) => {
                let mqp = &pq[0].1;
                assert_eq!(mqp.log_name, "mylog");
                assert_eq!(mqp.read_all, true);
                match mqp.limit {
                    Some(l) => assert_eq!(l, 10),
                    None => panic!("NO LIMIT FOUND"),
                }
            }
            _ => panic!("error"),
        }
    }

    #[test]
    fn process_positional_fields_select() {
        let access_token = VALID_TOKEN.to_string();

        let cfg = get_ds_log_auth_config_for("mylog".to_string(), &access_token);
        let cfg = Arc::new(RwLock::new(cfg));
        let query_c = Query::new(cfg);

        let query = "SELECT $1, $4 FROM mylog".to_string();
        let ast = query_c.parse_query(query.clone()).unwrap();
        let queries_parse = query_c.process_sql(&access_token, ast);

        match queries_parse {
            Ok(pq) => {
                let mqp = &pq[0].1;
                assert_eq!(mqp.log_name, "mylog");
                assert_eq!(
                    mqp.positional_fields,
                    vec![
                        PositionalColumn {
                            position: 1,
                            alias: "$1".to_string(),
                        },
                        PositionalColumn {
                            position: 4,
                            alias: "$4".to_string(),
                        }
                    ]
                )
            }
            _ => panic!("error"),
        }
    }

    #[test]
    fn process_positional_fields_select_limit() {
        let access_token = VALID_TOKEN.to_string();

        let cfg = get_ds_log_auth_config_for("mylog".to_string(), &access_token);
        let cfg = Arc::new(RwLock::new(cfg));
        let query_c = Query::new(cfg);

        let query = "SELECT $1, $4 FROM mylog LIMIT 10".to_string();
        let ast = query_c.parse_query(query.clone()).unwrap();
        let queries_parse = query_c.process_sql(&access_token, ast);

        match queries_parse {
            Ok(pq) => {
                let mqp = &pq[0].1;
                assert_eq!(mqp.log_name, "mylog");
                assert_eq!(
                    mqp.positional_fields,
                    vec![
                        PositionalColumn {
                            position: 1,
                            alias: "$1".to_string(),
                        },
                        PositionalColumn {
                            position: 4,
                            alias: "$4".to_string(),
                        }
                    ]
                );
                assert_eq!(
                    mqp.projections_ordered,
                    vec!["$1".to_string(), "$4".to_string()],
                    "Order of fields is incorrect"
                );
                match mqp.limit {
                    Some(l) => assert_eq!(l, 10),
                    None => panic!("NO LIMIT FOUND"),
                }
            }
            _ => panic!("error"),
        }
    }

    #[test]
    fn process_smart_fields_select_limit() {
        let access_token = VALID_TOKEN.to_string();

        let cfg = get_ds_log_auth_config_for("mylog".to_string(), &access_token);
        let cfg = Arc::new(RwLock::new(cfg));
        let query_c = Query::new(cfg);

        let query = "SELECT $ip, $email FROM mylog LIMIT 10".to_string();
        let ast = query_c.parse_query(query.clone()).unwrap();
        let queries_parse = query_c.process_sql(&access_token, ast);

        match queries_parse {
            Ok(pq) => {
                let mqp = &pq[0].1;
                assert_eq!(mqp.log_name, "mylog");
                assert_eq!(
                    mqp.smart_fields,
                    vec![
                        SmartColumn {
                            typed: "$ip".to_string(),
                            position: 1,
                            alias: "$ip".to_string(),
                        },
                        SmartColumn {
                            typed: "$email".to_string(),
                            position: 1,
                            alias: "$email".to_string(),
                        }
                    ]
                );
                assert_eq!(
                    mqp.projections_ordered,
                    vec!["$ip".to_string(), "$email".to_string()],
                    "Order of fields is incorrect"
                );
                assert_eq!(
                    mqp.scan_flags,
                    ScanFlags::IP | ScanFlags::EMAIL,
                    "Scan flags don't match"
                );
                match mqp.limit {
                    Some(l) => assert_eq!(l, 10),
                    None => panic!("NO LIMIT FOUND"),
                }
            }
            _ => panic!("error"),
        }
    }

    #[test]
    fn process_mixed_smart_positional_fields_select_limit() {
        let access_token = VALID_TOKEN.to_string();

        let cfg = get_ds_log_auth_config_for("mylog".to_string(), &access_token);
        let cfg = Arc::new(RwLock::new(cfg));
        let query_c = Query::new(cfg);

        let query = "SELECT $2, $ip, $email FROM mylog LIMIT 10".to_string();
        let ast = query_c.parse_query(query.clone()).unwrap();
        let queries_parse = query_c.process_sql(&access_token, ast);

        match queries_parse {
            Ok(pq) => {
                let mqp = &pq[0].1;
                assert_eq!(mqp.log_name, "mylog");
                assert_eq!(
                    mqp.smart_fields,
                    vec![
                        SmartColumn {
                            typed: "$ip".to_string(),
                            position: 1,
                            alias: "$ip".to_string(),
                        },
                        SmartColumn {
                            typed: "$email".to_string(),
                            position: 1,
                            alias: "$email".to_string(),
                        }
                    ]
                );
                assert_eq!(
                    mqp.positional_fields,
                    vec![PositionalColumn {
                        position: 2,
                        alias: "$2".to_string(),
                    }]
                );
                assert_eq!(
                    mqp.projections_ordered,
                    vec!["$2".to_string(), "$ip".to_string(), "$email".to_string()],
                    "Order of fields is incorrect"
                );
                match mqp.limit {
                    Some(l) => assert_eq!(l, 10),
                    None => panic!("NO LIMIT FOUND"),
                }
            }
            e => panic!("error parsing query: {:?}", e),
        }
    }

    #[test]
    #[should_panic]
    fn process_invalid_query() {
        let access_token = VALID_TOKEN.to_string();

        let cfg = get_ds_log_auth_config_for("mylog".to_string(), &access_token);
        let cfg = Arc::new(RwLock::new(cfg));
        let query_c = Query::new(cfg);

        let query = "INSERT INTO mylog ($line) VALES ('line')".to_string();
        if let Err(_) = query_c.parse_query(query.clone()) {
            panic!("Expected invalid query");
        }
    }

    #[test]
    fn process_simple_select_invalid_access() {
        let provided_access_token = VALID_TOKEN2.to_string();
        let access_token = VALID_TOKEN.to_string();

        let cfg = get_ds_log_auth_config_for("mylog".to_string(), &access_token);
        let cfg = Arc::new(RwLock::new(cfg));
        let query_c = Query::new(cfg);

        let query = "SELECT * FROM mylog".to_string();
        let ast = query_c.parse_query(query.clone()).unwrap();
        let queries_parse = query_c.process_sql(&provided_access_token, ast);

        match queries_parse {
            Ok(pq) => {
                let mqp = &pq[0].1;
                assert_eq!(mqp.log_name, "mylog");
                assert_eq!(mqp.read_all, true);
            }
            Err(e) => match e {
                ProcessingQueryError::Unauthorized(_) => assert!(true),
                _ => panic!("Incorrect error"),
            },
        }
    }

    #[test]
    fn process_simple_select_invalid_table() {
        let provided_access_token = VALID_TOKEN2.to_string();
        let access_token = VALID_TOKEN.to_string();

        let cfg = get_ds_log_auth_config_for("mylog".to_string(), &access_token);
        let cfg = Arc::new(RwLock::new(cfg));
        let query_c = Query::new(cfg);

        let query = "SELECT * FROM incorrect_log".to_string();
        let ast = query_c.parse_query(query.clone()).unwrap();
        let queries_parse = query_c.process_sql(&provided_access_token, ast);

        match queries_parse {
            Ok(pq) => {
                let mqp = &pq[0].1;
                assert_eq!(mqp.log_name, "mylog");
                assert_eq!(mqp.read_all, true);
            }
            Err(e) => match e {
                ProcessingQueryError::Unauthorized(_) => assert!(true),
                _ => panic!("Incorrect error"),
            },
        }
    }

    #[test]
    fn validate_invalid_table() {
        let access_token = VALID_TOKEN.to_string();

        let cfg = get_ds_log_auth_config_for("mylog".to_string(), &access_token);
        let cfg = Arc::new(RwLock::new(cfg));
        let query_c = Query::new(cfg);

        let query = "SELECT * FROM incorrect_log".to_string();
        let ast = query_c.parse_query(query.clone()).unwrap();
        match query_c.validate_logs(&ast) {
            None => panic!("Should have reported an error"),
            Some(_) => assert!(true),
        }
    }
}
