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
use std::iter;
use std::sync::{Arc, RwLock};

use futures::future::Either;
use futures::stream::Stream;
use futures::{future, Future};
use hyper::{header, Body, Chunk, Request, Response};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

use crate::api::{ListResponse, SafeOutput, ViewSet};
use crate::config::{Config, Token};
use crate::http::{return_400, return_404, return_500, ResponseFuture};
use crate::storage::{delete_object_metabucket, put_object_metabucket};

pub struct ApiTokens {
    config: Arc<RwLock<Config>>,
}

impl SafeOutput for Token {
    fn safe(&mut self) {
        self.secret_key = "*********".to_string();
    }
}

impl ApiTokens {
    pub fn new(cfg: Arc<RwLock<Config>>) -> ApiTokens {
        ApiTokens { config: cfg }
    }

    // Parses the token from the create body; returns error response in
    // case it is not valid.
    fn parse_create_body(
        entire_body: Vec<u8>,
        cfg: Arc<RwLock<Config>>,
    ) -> Result<Token, Response<Body>> {
        let payload: String = match String::from_utf8(entire_body.to_vec()) {
            Ok(str) => str,
            Err(_) => {
                return Err(return_400("Could not understand request"));
            }
        };
        //default token
        let mut new_token: Token = Token {
            access_key: "".to_string(),
            secret_key: "".to_string(),
            description: None,
            is_admin: false,
            enabled: true,
        };

        let token: serde_json::Value = match serde_json::from_str(&payload) {
            Ok(v) => v,
            Err(_) => {
                return Err(return_400("Could not parse request"));
            }
        };

        // Validate Access/Secret
        if let Some(serde_json::Value::String(access_key)) = token.get("access_key") {
            new_token.access_key = access_key.clone();
        }
        if let Some(serde_json::Value::String(secret_key)) = token.get("secret_key") {
            new_token.secret_key = secret_key.clone();
        }

        if let Some(serde_json::Value::String(description)) = token.get("description") {
            if description == "" {
                new_token.description = None;
            } else {
                new_token.description = Some(description.clone());
            }
        }

        if let Some(serde_json::Value::Bool(is_admin)) = token.get("is_admin") {
            new_token.is_admin = is_admin.clone();
        }

        if let Some(serde_json::Value::Bool(enabled)) = token.get("enabled") {
            new_token.enabled = enabled.clone();
        }

        // Validate Access/Secret
        if new_token.access_key == "" || new_token.secret_key == "" {
            // auto generate a token access_key
            let mut rng = thread_rng();
            if new_token.access_key == "" {
                // generate a 16 character long random string
                new_token.access_key = iter::repeat(())
                    .map(|()| rng.sample(Alphanumeric))
                    .take(16)
                    .collect::<String>()
                    .to_lowercase();
            }
            if new_token.secret_key == "" {
                // generate a 32 character long random string
                new_token.secret_key = iter::repeat(())
                    .map(|()| rng.sample(Alphanumeric))
                    .take(32)
                    .collect::<String>()
                    .to_lowercase();
            }
        }
        // Validate Access/Secret
        if new_token.access_key.len() != 16 || new_token.secret_key.len() != 32 {
            return Err(return_400(
                "Access/Secret key has an invalid length. (Access 16, Secret 32)",
            ));
        }

        let cfg_read = cfg.read().unwrap();

        // validate token access_key
        if cfg_read.tokens.contains_key(&new_token.access_key) {
            return Err(return_400("Token access key already in use"));
        }
        Ok(new_token)
    }

    fn parse_update_body(
        entire_body: Vec<u8>,
        cfg: Arc<RwLock<Config>>,
        pk: &String,
    ) -> Result<Token, Response<Body>> {
        let payload: String = match String::from_utf8(entire_body.to_vec()) {
            Ok(str) => str,
            Err(_) => {
                return Err(return_400("Could not understand request"));
            }
        };
        let cfg_read = cfg.read().unwrap();
        let mut current_token = match cfg_read.tokens.get(pk) {
            Some(v) => v.clone(),
            None => {
                return Err(return_404());
            }
        };

        let token: serde_json::Value = match serde_json::from_str(&payload) {
            Ok(v) => v,
            Err(_) => {
                return Err(return_400("Could not parse request"));
            }
        };

        // Validate Access/Secret
        if let Some(serde_json::Value::String(access_key)) = token.get("access_key") {
            if *access_key != current_token.access_key {
                return Err(return_400("Access Key cannot be changed."));
            }
        }
        if let Some(serde_json::Value::String(secret_key)) = token.get("secret_key") {
            if *secret_key != current_token.secret_key {
                return Err(return_400("Secret Key cannot be changed."));
            }
        }

        if let Some(serde_json::Value::String(description)) = token.get("description") {
            if description == "" {
                current_token.description = None;
            } else {
                current_token.description = Some(description.clone());
            }
        }

        if let Some(serde_json::Value::Bool(is_admin)) = token.get("is_admin") {
            current_token.is_admin = is_admin.clone();
        }

        if let Some(serde_json::Value::Bool(enabled)) = token.get("enabled") {
            current_token.enabled = enabled.clone();
        }
        Ok(current_token)
    }
}

impl ViewSet for ApiTokens {
    fn list(&self, _req: Request<Body>) -> ResponseFuture {
        let cfg_read = self.config.read().unwrap();
        let mut tokens: Vec<Token> = Vec::new();
        for (_, token) in &cfg_read.tokens {
            tokens.push(token.clone());
        }
        let items = ListResponse {
            total: cfg_read.tokens.len(),
            next: None,
            previous: None,
            results: tokens,
        };
        Box::new(self.build_response(items))
    }

    fn create(&self, req: Request<Body>) -> ResponseFuture {
        let cfg = Arc::clone(&self.config);
        let cfg2 = Arc::clone(&self.config);
        Box::new(
            req.into_body()
                .concat2()
                .from_err()
                .and_then(move |entire_body| {
                    match ApiTokens::parse_create_body(entire_body.to_vec(), cfg) {
                        Ok(mut new_token) => {
                            // everything seems ok, create the token
                            let token_serialized = serde_json::to_string(&new_token).unwrap();
                            let resp = put_object_metabucket(
                                cfg2,
                                format!("minsql/meta/tokens/{}", &new_token.access_key),
                                token_serialized,
                            )
                            .then(move |v| match v {
                                Ok(_) => {
                                    new_token.safe();
                                    let ds_serialized = serde_json::to_string(&new_token).unwrap();

                                    let body = Body::from(Chunk::from(ds_serialized));
                                    let mut response = Response::builder();
                                    response.header(header::CONTENT_TYPE, "application/json");

                                    future::ok(response.body(body).unwrap())
                                }
                                Err(_) => future::ok(return_500("error saving new token")),
                            });
                            Either::A(resp)
                        }
                        Err(e) => Either::B(future::ok(e)),
                    }
                }),
        )
    }

    fn retrieve(&self, _req: Request<Body>, pk: &str) -> ResponseFuture {
        let cfg_read = self.config.read().unwrap();
        let mut token = match cfg_read.tokens.get(pk) {
            Some(ds) => ds.clone(),
            None => {
                return Box::new(future::ok(return_404()));
            }
        };
        token.safe();
        self.build_response(token)
    }

    fn update(&self, req: Request<Body>, pk: &str) -> ResponseFuture {
        let pk = pk.to_string();
        let cfg = Arc::clone(&self.config);
        let cfg2 = Arc::clone(&self.config);
        Box::new(
            req.into_body()
                .concat2()
                .from_err()
                .and_then(move |entire_body| {
                    match ApiTokens::parse_update_body(entire_body.to_vec(), cfg, &pk) {
                        Ok(mut current_token) => {
                            // everything seems ok, write to token
                            let ds_serialized = serde_json::to_string(&current_token).unwrap();
                            let res = put_object_metabucket(
                                cfg2,
                                format!("minsql/meta/tokens/{}", pk),
                                ds_serialized.clone(),
                            )
                            .map_err(|_| {})
                            .then(move |v| match v {
                                Ok(_) => {
                                    //remove sensitive data
                                    current_token.safe();
                                    let ds_serialized =
                                        serde_json::to_string(&current_token).unwrap();

                                    let body = Body::from(Chunk::from(ds_serialized));
                                    let mut response = Response::builder();
                                    response.header(header::CONTENT_TYPE, "application/json");

                                    future::ok(response.body(body).unwrap())
                                }
                                Err(_) => future::ok(return_500("error saving token")),
                            });
                            Either::A(res)
                        }
                        Err(e) => Either::B(future::ok(e)),
                    }
                }),
        )
    }

    fn delete(&self, _req: Request<Body>, pk: &str) -> ResponseFuture {
        let cfg_read = self.config.read().unwrap();
        let mut token = match cfg_read.tokens.get(pk) {
            Some(v) => v.clone(),
            None => {
                return Box::new(future::ok(return_404()));
            }
        };

        let token_access_key = token.access_key.clone();

        let cfg = Arc::clone(&self.config);
        Box::new(
            delete_object_metabucket(cfg, format!("minsql/meta/tokens/{}", token_access_key))
                .map_err(|_| {})
                .then(move |v| match v {
                    Ok(_) => {
                        //remove sensitive data
                        token.safe();
                        let ds_serialized = serde_json::to_string(&token).unwrap();
                        let body = Body::from(Chunk::from(ds_serialized));
                        let mut response = Response::builder();
                        response.header(header::CONTENT_TYPE, "application/json");

                        future::ok(response.body(body).unwrap())
                    }
                    Err(_) => future::ok(return_500("Error deleting token from storage")),
                }),
        )
    }
}
