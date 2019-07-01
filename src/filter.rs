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

use sqlparser::sqlast::{ASTNode, SQLBinaryOperator, SQLStatement};

pub fn line_fails_query_conditions(
    line: &String,
    query: &SQLStatement,
    projection_values: &HashMap<String, Option<String>>,
) -> bool {
    let mut skip_line = false;
    match query {
        sqlparser::sqlast::SQLStatement::SQLQuery(ref q) => match q.body {
            sqlparser::sqlast::SQLSetExpr::Select(ref bodyselect) => match &bodyselect.selection {
                Some(slct) => {
                    let all_conditions_pass = evaluate(&slct, projection_values, line);
                    return !all_conditions_pass;
                }
                None => {
                    skip_line = false;
                }
            },
            _ => {}
        },
        _ => {}
    };
    return skip_line;
}

pub fn evaluate(
    ast_node: &ASTNode,
    projection_values: &HashMap<String, Option<String>>,
    line: &String,
) -> bool {
    match ast_node {
        ASTNode::SQLNested(nested_ast) => {
            return evaluate(&nested_ast, projection_values, line);
        }
        ASTNode::SQLIsNotNull(ast) => {
            let identifier = get_identifier_from_ast(&ast);
            if projection_values.contains_key(&identifier[..]) == false
                || projection_values[&identifier].is_none()
            {
                return false;
            }
            return true;
        }
        ASTNode::SQLIsNull(ast) => {
            let identifier = get_identifier_from_ast(&ast);
            if !projection_values[&identifier].is_none() {
                return false;
            }
            return true;
        }
        ASTNode::SQLBinaryOp { left, op, right } => {
            let identifier = left.to_string();
            match op {
                SQLBinaryOperator::And => {
                    let left_eval = evaluate(&left, projection_values, line);
                    let right_eval = evaluate(&right, projection_values, line);
                    return left_eval && right_eval;
                }
                SQLBinaryOperator::Or => {
                    let left_eval = evaluate(&left, projection_values, line);
                    let right_eval = evaluate(&right, projection_values, line);
                    return left_eval || right_eval;
                }
                SQLBinaryOperator::Eq => {
                    if identifier != "$line"
                        && projection_values.contains_key(&identifier[..]) == false
                    {
                        return false;
                    }

                    // TODO: Optimize this op_value preparation, don't do it in the loop
                    let op_value = match **right {
                        ASTNode::SQLIdentifier(ref right_value) => {
                            // Did they used double quotes for the value?
                            let mut str_id = right_value.to_string();
                            if str_id.starts_with("\"") {
                                str_id = str_id[1..][..str_id.len() - 2].to_string();
                            }
                            str_id
                        }
                        ASTNode::SQLValue(ref right_value) => match right_value {
                            sqlparser::sqlast::Value::SingleQuotedString(s) => s.to_string(),
                            _ => right_value.to_string(),
                        },
                        _ => "".to_string(),
                    };

                    let pval = projection_values.get(&identifier).unwrap();
                    match pval {
                        Some(ref s) => {
                            if s == &op_value {
                                return true;
                            }
                        }
                        None => {
                            return false;
                        }
                    };
                    return false;
                }
                SQLBinaryOperator::NotEq => {
                    if identifier != "$line"
                        && projection_values.contains_key(&identifier[..]) == false
                    {
                        return false;
                    }
                    // TODO: Optimize this op_value preparation, don't do it in the loop
                    let op_value = match **right {
                        ASTNode::SQLIdentifier(ref right_value) => {
                            // Did they used double quotes for the value?
                            let mut str_id = right_value.to_string();
                            if str_id.starts_with("\"") {
                                str_id = str_id[1..][..str_id.len() - 2].to_string();
                            }
                            str_id
                        }
                        ASTNode::SQLValue(ref right_value) => match right_value {
                            sqlparser::sqlast::Value::SingleQuotedString(s) => s.to_string(),
                            _ => right_value.to_string(),
                        },
                        _ => "".to_string(),
                    };
                    let pval = projection_values.get(&identifier).unwrap();
                    match pval {
                        Some(ref s) => {
                            if s == &op_value {
                                return false;
                            }
                        }
                        None => {
                            return false;
                        }
                    };
                    return true;
                }
                SQLBinaryOperator::Like => {
                    if identifier != "$line"
                        && projection_values.contains_key(&identifier[..]) == false
                    {
                        return false;
                    }
                    // TODO: Optimize this op_value preparation, don't do it in the loop
                    let op_value = match **right {
                        ASTNode::SQLIdentifier(ref right_value) => {
                            // Did they used double quotes for the value?
                            let mut str_id = right_value.to_string();
                            if str_id.starts_with("\"") {
                                str_id = str_id[1..][..str_id.len() - 2].to_string();
                            }
                            str_id
                        }
                        ASTNode::SQLValue(ref right_value) => match right_value {
                            sqlparser::sqlast::Value::SingleQuotedString(s) => s.to_string(),
                            _ => right_value.to_string(),
                        },
                        _ => "".to_string(),
                    };
                    // TODO: Add support for wildcards ie: LIKE 'server_.domain.com' where _ is a single character wildcard
                    if identifier == "$line" {
                        return line.contains(&op_value[..]);
                    } else {
                        let pval = projection_values.get(&identifier).unwrap();
                        match pval {
                            Some(ref s) => {
                                if s.contains(&op_value) == false {
                                    return false;
                                }
                            }
                            None => {
                                return false;
                            }
                        };
                        return true;
                    }
                }
                SQLBinaryOperator::NotLike => {
                    if identifier != "$line"
                        && projection_values.contains_key(&identifier[..]) == false
                    {
                        return false;
                    }
                    // TODO: Optimize this op_value preparation, don't do it in the loop
                    let op_value = match **right {
                        ASTNode::SQLIdentifier(ref right_value) => {
                            // Did they used double quotes for the value?
                            let mut str_id = right_value.to_string();
                            if str_id.starts_with("\"") {
                                str_id = str_id[1..][..str_id.len() - 2].to_string();
                            }
                            str_id
                        }
                        ASTNode::SQLValue(ref right_value) => match right_value {
                            sqlparser::sqlast::Value::SingleQuotedString(s) => s.to_string(),
                            _ => right_value.to_string(),
                        },
                        _ => "".to_string(),
                    };
                    // TODO: Add support for wildcards ie: LIKE 'server_.domain.com' where _ is a single character wildcard
                    if identifier == "$line" {
                        return !line.contains(&op_value[..]);
                    } else {
                        let pval = projection_values.get(&identifier).unwrap();
                        match pval {
                            Some(ref s) => {
                                if s.contains(&op_value) == true {
                                    return false;
                                }
                            }
                            None => {
                                return false;
                            }
                        };
                        return true;
                    }
                }
                xop => {
                    return false;
                }
            }
        }
        x => {
            return false;
        }
    };
}

/// Extracts an `ASTNode` identifier as a `String`
pub fn get_identifier_from_ast(ast: &ASTNode) -> String {
    match ast {
        ASTNode::SQLIdentifier(ref identifier) => identifier.to_string(),
        _ => {
            // TODO: Should we be retunring anything at all?
            "".to_string()
        }
    }
}

#[cfg(test)]
mod filter_tests {
    use std::sync::{Arc, RwLock};

    use crate::config::{Config, Log, LogAuth};
    use crate::query::{extract_positional_fields, extract_smart_fields, Query};

    use super::*;

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
            log: log_map,
            auth: auth,
        };
        cfg
    }

    fn setup_select(
        query_stmt: String,
        line: &String,
    ) -> (SQLStatement, HashMap<String, Option<String>>) {
        let access_token = "TOKEN1".to_string();

        let cfg = get_ds_log_auth_config_for("mylog".to_string(), &access_token);
        let cfg = Arc::new(RwLock::new(cfg));
        let query_c = Query::new(cfg);

        let qparse = query_c.parse_query(query_stmt).unwrap();
        let qparsing = query_c.process_sql(&access_token, qparse).unwrap();
        let query = &qparsing.get(0).unwrap().0;
        let query_data = &qparsing.get(0).unwrap().1;
        let mut projection_values: HashMap<String, Option<String>> = HashMap::new();
        // Extract projections
        extract_positional_fields(&mut projection_values, query_data, &line);
        extract_smart_fields(&mut projection_values, query_data, &line);
        return (query.clone(), projection_values);
    }

    struct FilterTestCase {
        query_stmt: String,
        line: String,
        expected_pass: bool,
    }

    fn run_test(ftc: FilterTestCase) {
        let (query, projection_values) = setup_select(ftc.query_stmt, &ftc.line);

        let skip_line = line_fails_query_conditions(&ftc.line, &query, &projection_values);
        assert_eq!(!skip_line, ftc.expected_pass);
    }

    #[test]
    fn get_identifier_from_ast_node() {
        let ast_node = ASTNode::SQLIdentifier("test_id".to_owned());
        let identifier = get_identifier_from_ast(&ast_node);
        assert_eq!(identifier, "test_id");
    }

    #[test]
    fn invalid_identifier_from_ast_node() {
        let ast_node = ASTNode::SQLWildcard;
        let identifier = get_identifier_from_ast(&ast_node);
        assert_eq!(identifier, "");
    }

    #[test]
    fn select_eq() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip='192.168.0.1'".to_string(),
            line: "192.168.0.1 \"quoted\"".to_string(),
            expected_pass: true,
        });
    }

    #[test]
    fn select_eq_fail() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip='192.168.0.1'".to_string(),
            line: "192.168.0.2 \"quoted\"".to_string(),
            expected_pass: false,
        });
    }

    #[test]
    fn select_not_eq() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip!='192.168.0.1'".to_string(),
            line: "192.168.0.2 \"quoted\"".to_string(),
            expected_pass: true,
        });
    }

    #[test]
    fn select_not_eq_fail() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip!='192.168.0.1'".to_string(),
            line: "192.168.0.1 \"quoted\"".to_string(),
            expected_pass: false,
        });
    }

    #[test]
    fn select_line_like() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $line LIKE 'uo'".to_string(),
            line: "192.168.0.2 \"quoted\"".to_string(),
            expected_pass: true,
        });
    }

    #[test]
    fn select_line_like_fail() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $line LIKE 'zz'".to_string(),
            line: "192.168.0.2 \"quoted\"".to_string(),
            expected_pass: false,
        });
    }

    #[test]
    fn select_line_not_like() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $line NOT LIKE 'zz'".to_string(),
            line: "192.168.0.2 \"quoted\"".to_string(),
            expected_pass: true,
        });
    }

    #[test]
    fn select_line_not_like_fail() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $line NOT LIKE 'uo'".to_string(),
            line: "192.168.0.2 \"quoted\"".to_string(),
            expected_pass: false,
        });
    }

    #[test]
    fn select_sf_is_null() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip IS NULL".to_string(),
            line: "\"quoted\"".to_string(),
            expected_pass: true,
        });
    }

    #[test]
    fn select_sf_is_null_fail() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip IS NULL".to_string(),
            line: "192.168.0.2 \"quoted\"".to_string(),
            expected_pass: false,
        });
    }

    #[test]
    fn select_sf_not_null() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip IS NOT NULL".to_string(),
            line: "192.168.0.2 \"quoted\"".to_string(),
            expected_pass: true,
        });
    }

    #[test]
    fn select_sf_not_null_fail() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip IS NOT NULL".to_string(),
            line: "\"quoted\"".to_string(),
            expected_pass: false,
        });
    }

    #[test]
    fn select_and_eq() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip='192.168.0.1' AND $quoted='quoted'"
                .to_string(),
            line: "192.168.0.1 \"quoted\"".to_string(),
            expected_pass: true,
        });
    }

    #[test]
    fn select_and_eq_fail() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip='192.168.0.1' AND $quoted='quoted'"
                .to_string(),
            line: "192.168.0.2 \"quoted\"".to_string(),
            expected_pass: false,
        });
    }

    #[test]
    fn select_and_eq_single_quote_line() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip='192.168.0.1' AND $quoted='quoted'"
                .to_string(),
            line: "192.168.0.1 'quoted'".to_string(),
            expected_pass: true,
        });
    }

    #[test]
    fn select_and_eq_single_quote_line_fail() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip='192.168.0.1' AND $quoted='quoted2'"
                .to_string(),
            line: "192.168.0.1 'quoted'".to_string(),
            expected_pass: false,
        });
    }

    #[test]
    fn select_or_eq() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip='192.168.0.1' OR $ip='192.168.0.2'"
                .to_string(),
            line: "192.168.0.2 \"quoted\"".to_string(),
            expected_pass: true,
        });
    }

    #[test]
    fn select_or_eq_fail() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip='192.168.0.1' OR $ip='192.168.0.2'"
                .to_string(),
            line: "192.168.0.3 \"quoted\"".to_string(),
            expected_pass: false,
        });
    }

    #[test]
    fn select_and_or_eq() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip='192.168.0.1' AND $quoted='quoted' OR $quoted='quoted2'"
                .to_string(),
            line: "192.168.0.1 \"quoted2\"".to_string(),
            expected_pass: true,
        });
    }

    #[test]
    fn select_and_or_eq_fail() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip='192.168.0.1' OR $quoted='quoted' AND $quoted='quoted2'"
                .to_string(),
            line: "192.168.0.2 \"quoted2\"".to_string(),
            expected_pass: false,
        });
    }

    #[test]
    fn select_and_or_eq_nested() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip='192.168.0.1' OR ($quoted='quoted' AND $quoted='quoted2')"
                .to_string(),
            line: "192.168.0.1 \"quoted2\"".to_string(),
            expected_pass: true,
        });
    }

    #[test]
    fn select_and_or_eq_nested_fail() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $ip='192.168.0.1' AND ($quoted='quoted' OR $quoted='quoted2')"
                .to_string(),
            line: "192.168.0.1 \"quoted3\"".to_string(),
            expected_pass: false,
        });
    }

    #[test]
    fn select_and_or_eq_nested_repeated_smart_field() {
        run_test(FilterTestCase {
            query_stmt: "SELECT * FROM mylog WHERE $quoted='quoted' OR ($ip='192.168.0.1' AND $ip2='10.20.30.40')"
                .to_string(),
            line: "192.168.0.1 \"quoted2\" 10.20.30.40".to_string(),
            expected_pass: true,
        });
    }

}
