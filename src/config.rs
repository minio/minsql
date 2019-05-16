use std::collections::HashMap;
use std::env;
use std::fmt;
use std::fs;

//TODO: Remove serialize derive before commit

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub version: String,
    pub server: Option<Server>,
    pub datastore: Vec<DataStore>,
    pub log: Vec<Log>,
    pub auth: HashMap<String, HashMap<String, LogAuth>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Server {
    pub address: Option<String>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub ca_certs: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
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
    pub name: String,
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

#[derive(Debug)]
pub struct ConfigurationError {
    details: String
}

impl ConfigurationError {
    pub fn new(msg: &str) -> ConfigurationError {
        ConfigurationError { details: msg.to_string() }
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
        Err(_) => return Err(ConfigurationError::new("Could not read configuration file"))
    };
    // try to parse the toml string
    let configuration: Config = match toml::from_str(&contents) {
        Ok(t) => t,
        Err(e) => return Err(ConfigurationError::new(&format!("{}", e)[..]))
    };
    Ok(configuration)
}