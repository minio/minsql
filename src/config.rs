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
use std::env;
use std::fmt;

use log::error;
use serde_derive::{Deserialize, Serialize};

use crate::constants::{
    DEFAULT_SERVER_ADDRESS, METABUCKET_ACCESS_KEY, METABUCKET_ENDPOINT, METABUCKET_NAME,
    METABUCKET_SECRET_KEY, PKCS12_CERT, PKCS12_PASSWORD,
};
use clap::{App, Arg};

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub server: Server,
    #[serde(default = "HashMap::new")]
    pub datastore: HashMap<String, DataStore>,
    #[serde(default = "HashMap::new")]
    pub log: HashMap<String, Log>,
    #[serde(default = "HashMap::new")]
    pub auth: HashMap<String, HashMap<String, LogAuth>>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Server {
    pub address: String,
    pub metadata_endpoint: String,
    pub metadata_bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub pkcs12_cert: Option<String>,
    pub pkcs12_password: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct DataStore {
    pub name: Option<String>,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub bucket: String,
    pub prefix: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Log {
    pub name: Option<String>,
    pub datastores: Vec<String>,
    pub commit_window: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LogAuth {
    pub api: Vec<String>,
    pub expire: String,
    pub status: String,
}

impl Config {
    pub fn new(server: Server) -> Config {
        Config {
            server: server,
            datastore: HashMap::new(),
            log: HashMap::new(),
            auth: HashMap::new(),
        }
    }

    pub fn get_log(&self, logname: &String) -> Option<&Log> {
        self.log.get(&logname[..])
    }
    /// Translates a string duration to an unsigned integer
    /// for example, "5s" returns 5
    /// "10m" returns 600
    pub fn commit_window_to_seconds(commit_window: &String) -> u64 {
        let last_character = &commit_window[commit_window.len() - 1..commit_window.len()];
        match last_character {
            "s" => {
                let integer_value = &commit_window[0..commit_window.len() - 1].parse::<u64>();
                let seconds = match integer_value {
                    Ok(val) => *val,
                    Err(_) => {
                        error!("Interval cannot be parsed");
                        0 as u64
                    }
                };
                seconds
            }
            "m" => {
                let integer_value = &commit_window[0..commit_window.len() - 1].parse::<u64>();
                let seconds = match integer_value {
                    Ok(val) => *val * 60,
                    Err(_) => {
                        error!("Interval cannot be parsed");
                        0 as u64
                    }
                };
                seconds
            }
            _ => 0 as u64,
        }
    }
}

#[derive(Debug)]
pub struct ConfigurationError {
    details: String,
}

impl ConfigurationError {
    pub fn new(msg: &str) -> ConfigurationError {
        ConfigurationError {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for ConfigurationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

// Loads the configuration file from command arguments and the environment.
pub fn load_configuration() -> Result<Config, ConfigurationError> {
    //load arguments
    let matches = App::new("MinSQL")
        .version("1.0")
        .about("Log Search Engine")
        .arg(
            Arg::with_name("address")
                .takes_value(true)
                .default_value(DEFAULT_SERVER_ADDRESS)
                .short("a")
                .long("address")
                .help("Server binding address, i.e.: 0.0.0.0:9000")
                .required(true),
        )
        .get_matches();

    // Server address, safe to unwrap since it has a default value.
    let address = matches.value_of("address").unwrap().to_string();

    // Check for configuration on the environment, else return error.

    let metadata_endpoint: String = match env::var(METABUCKET_ENDPOINT) {
        Ok(val) => val,
        Err(e) => {
            return Err(ConfigurationError::new(&format!(
                "No meta bucket endpoint environment variable `{}` set. {}",
                METABUCKET_ENDPOINT, e
            )));
        }
    };

    let metadata_bucket: String = match env::var(METABUCKET_NAME) {
        Ok(val) => val,
        Err(e) => {
            return Err(ConfigurationError::new(&format!(
                "No meta bucket name environment variable `{}` set. {}",
                METABUCKET_NAME, e
            )));
        }
    };

    let access_key: String = match env::var(METABUCKET_ACCESS_KEY) {
        Ok(val) => val,
        Err(e) => {
            return Err(ConfigurationError::new(&format!(
                "No meta bucket endpoint environment variable `{}` set. {}",
                METABUCKET_ACCESS_KEY, e
            )));
        }
    };

    let secret_key: String = match env::var(METABUCKET_SECRET_KEY) {
        Ok(val) => val,
        Err(e) => {
            return Err(ConfigurationError::new(&format!(
                "No meta bucket endpoint environment variable `{}` set. {}",
                METABUCKET_SECRET_KEY, e
            )));
        }
    };

    // Certificates are optional.

    let pkcs12_cert: Option<String> = match env::var(PKCS12_CERT) {
        Ok(val) => Some(val),
        Err(_) => None,
    };

    let pkcs12_password: Option<String> = match env::var(PKCS12_PASSWORD) {
        Ok(val) => Some(val),
        Err(_) => None,
    };

    let server = Server {
        address,
        metadata_endpoint,
        metadata_bucket,
        access_key,
        secret_key,
        pkcs12_cert,
        pkcs12_password,
    };

    let mut configuration = Config::new(server);

    // store datasource names in the structs
    for (name, ds) in &mut configuration.datastore {
        ds.name = Some(name.clone());
    }
    // store log names in the structs
    for (name, log) in &mut configuration.log {
        log.name = Some(name.clone());
    }
    Ok(configuration)
}

#[cfg(test)]
mod config_tests {
    use crate::config::Config;

    #[test]
    fn parse_interval() {
        assert_eq!(Config::commit_window_to_seconds(&"5s".to_string()), 5);
        assert_eq!(Config::commit_window_to_seconds(&"5m".to_string()), 300);
    }
}
