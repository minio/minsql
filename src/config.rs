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
use std::fmt;
use std::fs;
use std::sync::{Arc, Once, RwLock, ONCE_INIT};
use std::{env, mem};

use log::error;
use serde_derive::{Deserialize, Serialize};

use crate::constants::DEFAULT_SERVER_ADDRESS;

/// Holds a single Configuration instance protected by a `RWLock`
#[derive(Clone)]
struct ConfigHolder {
    // Since we will be used in many threads, we need to protect
    // concurrent access
    cfg: Arc<RwLock<Config>>,
}
/// Returns a reference to a the single protected configuration
pub fn get_config() -> Arc<RwLock<Config>> {
    // Initialize it to a null value
    static mut SINGLETON: *const ConfigHolder = 0 as *const ConfigHolder;
    static ONCE: Once = ONCE_INIT;

    unsafe {
        ONCE.call_once(|| {
            // Make it
            let singleton = ConfigHolder {
                cfg: Arc::new(RwLock::new(Config {
                    version: "".to_owned(),
                    server: None,
                    datastore: Default::default(),
                    log: Default::default(),
                    auth: Default::default(),
                })),
            };

            // Put it in the heap so it can outlive this call
            SINGLETON = mem::transmute(Box::new(singleton));
        });

        // Now we give out a counted reference of the data that is safe to use concurrently.
        Arc::clone(&(*SINGLETON).cfg)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub version: String,
    pub server: Option<Server>,
    #[serde(default = "HashMap::new")]
    pub datastore: HashMap<String, DataStore>,
    #[serde(default = "HashMap::new")]
    pub log: HashMap<String, Log>,
    #[serde(default = "HashMap::new")]
    pub auth: HashMap<String, HashMap<String, LogAuth>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Server {
    pub address: Option<String>,
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

#[derive(Serialize, Deserialize, Clone)]
pub struct Log {
    pub name: Option<String>,
    pub datastores: Vec<String>,
    pub commit_window: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LogAuth {
    pub token: String,
    pub api: Vec<String>,
    pub expire: String,
    pub status: String,
}

impl Config {
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

    /// Returns the server address to bind, if no configuration is found it returns the default
    /// address of 0.0.0.0:9999
    pub fn get_server_address(&self) -> String {
        match &self.server {
            Some(server) => match &server.address {
                Some(address) => address.clone(),
                None => DEFAULT_SERVER_ADDRESS.to_owned(),
            },
            None => DEFAULT_SERVER_ADDRESS.to_owned(),
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

// Loads the configuration file from command arguments or defaults to config.toml
pub fn load_configuration() -> Result<Config, ConfigurationError> {
    //load arguments
    let args: Vec<String> = env::args().collect();
    // We default to loading a config.toml if minsql is run without arguments
    let mut filename = "config.toml";
    if args.len() > 1 {
        filename = args[1].as_str();
    }
    // try to read the file
    let contents = match fs::read_to_string(filename) {
        Ok(f) => f,
        Err(_) => return Err(ConfigurationError::new("Could not read configuration file")),
    };
    // try to parse the toml string
    let mut configuration: Config = match toml::from_str(&contents) {
        Ok(t) => t,
        Err(e) => return Err(ConfigurationError::new(&format!("{}", e)[..])),
    };
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
