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

use crate::config::Config;
use std::sync::{Arc, RwLock};

pub struct Auth {
    config: Arc<RwLock<Config>>,
}

impl Auth {
    pub fn new(cfg: Arc<RwLock<Config>>) -> Auth {
        Auth { config: cfg }
    }
    /// Checks the configuration hierarchy to validate if a token has access to a log
    pub fn token_has_access_to_log(&self, access_token: &str, log_name: &str) -> bool {
        let cfg = self.config.read().unwrap();
        match cfg.auth.get(access_token) {
            Some(val) => match val.get(log_name) {
                Some(_) => return true,
                None => return false,
            },
            None => return false,
        }
    }
}

#[cfg(test)]
mod auth_tests {
    use std::collections::HashMap;

    use crate::config::{Config, LogAuth};

    use super::*;

    // Generates a Config object with only one auth item for one log
    fn get_auth_config_for(token: String, log_name: String) -> Config {
        let mut log_auth_map: HashMap<String, LogAuth> = HashMap::new();
        log_auth_map.insert(
            log_name,
            LogAuth {
                token: token.clone(),
                api: Vec::new(),
                expire: "".to_string(),
                status: "".to_string(),
            },
        );

        let mut auth = HashMap::new();
        auth.insert(token.clone(), log_auth_map);

        let cfg = Config {
            version: "1".to_string(),
            server: None,
            datastore: HashMap::new(),
            log: HashMap::new(),
            auth: auth,
        };
        cfg
    }

    struct TokenTestCase {
        token: String,
        log_name: String,
        valid_token: String,
        valid_log_name: String,
        expected: bool,
    }

    fn run_test_get_auth_config_for(test_case: TokenTestCase) {
        let cfg = get_auth_config_for(test_case.valid_token, test_case.valid_log_name);
        // override the config
        let cfg = Arc::new(RwLock::new(cfg));
        let auth_c = Auth::new(cfg);

        let result = auth_c.token_has_access_to_log(&test_case.token[..], &test_case.log_name[..]);

        assert_eq!(result, test_case.expected);
    }

    #[test]
    fn valid_token() {
        run_test_get_auth_config_for(TokenTestCase {
            valid_token: "TOKEN1".to_string(),
            valid_log_name: "mylog".to_string(),

            token: "TOKEN1".to_string(),
            log_name: "mylog".to_string(),

            expected: true,
        })
    }

    #[test]
    fn invalid_token() {
        run_test_get_auth_config_for(TokenTestCase {
            valid_token: "TOKEN1".to_string(),
            valid_log_name: "mylog".to_string(),

            token: "INVALID".to_string(),
            log_name: "mylog".to_string(),

            expected: false,
        })
    }

    #[test]
    fn valid_token_invalid_log() {
        run_test_get_auth_config_for(TokenTestCase {
            valid_token: "TOKEN1".to_string(),
            valid_log_name: "mylog".to_string(),

            token: "TOKEN1".to_string(),
            log_name: "invalid_log".to_string(),

            expected: false,
        })
    }
}
