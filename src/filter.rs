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

use log::info;
use sqlparser::sqlast::{ASTNode, SQLStatement};

pub fn line_fails_query_conditions(
    line: &String,
    query: &SQLStatement,
    projection_values: &HashMap<String, String>,
) -> bool {
    let mut skip_line = false;
    match query {
        sqlparser::sqlast::SQLStatement::SQLQuery(ref q) => {
            match q.body {
                sqlparser::sqlast::SQLSetExpr::Select(ref bodyselect) => {
                    let mut all_conditions_pass = true;
                    for slct in &bodyselect.selection {
                        match slct {
                            sqlparser::sqlast::ASTNode::SQLIsNotNull(ast) => {
                                let identifier = get_identifier_from_ast(&ast);
                                if projection_values.contains_key(&identifier[..]) == false
                                    || projection_values[&identifier] == ""
                                {
                                    all_conditions_pass = false;
                                }
                            }
                            sqlparser::sqlast::ASTNode::SQLIsNull(ast) => {
                                let identifier = get_identifier_from_ast(&ast);
                                if projection_values[&identifier] != "" {
                                    all_conditions_pass = false;
                                }
                            }
                            sqlparser::sqlast::ASTNode::SQLBinaryOp { left, op, right } => {
                                let identifier = left.to_string();

                                match op {
                                    sqlparser::sqlast::SQLBinaryOperator::Eq => {
                                        // TODO: Optimize this op_value preparation, don't do it in the loop
                                        let op_value = match **right {
                                            sqlparser::sqlast::ASTNode::SQLIdentifier(
                                                ref right_value,
                                            ) => {
                                                // Did they used double quotes for the value?
                                                let mut str_id = right_value.to_string();
                                                if str_id.starts_with("\"") {
                                                    str_id =
                                                        str_id[1..][..str_id.len() - 2].to_string();
                                                }
                                                str_id
                                            }
                                            sqlparser::sqlast::ASTNode::SQLValue(
                                                ref right_value,
                                            ) => match right_value {
                                                sqlparser::sqlast::Value::SingleQuotedString(s) => {
                                                    s.to_string()
                                                }
                                                _ => right_value.to_string(),
                                            },
                                            _ => "".to_string(),
                                        };

                                        if projection_values.contains_key(&identifier[..])
                                            && projection_values[&identifier] != op_value
                                        {
                                            all_conditions_pass = false;
                                        }
                                    }
                                    sqlparser::sqlast::SQLBinaryOperator::NotEq => {
                                        // TODO: Optimize this op_value preparation, don't do it in the loop
                                        let op_value = match **right {
                                            sqlparser::sqlast::ASTNode::SQLIdentifier(
                                                ref right_value,
                                            ) => {
                                                // Did they used double quotes for the value?
                                                let mut str_id = right_value.to_string();
                                                if str_id.starts_with("\"") {
                                                    str_id =
                                                        str_id[1..][..str_id.len() - 2].to_string();
                                                }
                                                str_id
                                            }
                                            sqlparser::sqlast::ASTNode::SQLValue(
                                                ref right_value,
                                            ) => match right_value {
                                                sqlparser::sqlast::Value::SingleQuotedString(s) => {
                                                    s.to_string()
                                                }
                                                _ => right_value.to_string(),
                                            },
                                            _ => "".to_string(),
                                        };
                                        if projection_values.contains_key(&identifier[..])
                                            && projection_values[&identifier] == op_value
                                        {
                                            all_conditions_pass = false;
                                        }
                                    }
                                    sqlparser::sqlast::SQLBinaryOperator::Like => {
                                        // TODO: Optimize this op_value preparation, don't do it in the loop
                                        let op_value = match **right {
                                            sqlparser::sqlast::ASTNode::SQLIdentifier(
                                                ref right_value,
                                            ) => {
                                                // Did they used double quotes for the value?
                                                let mut str_id = right_value.to_string();
                                                if str_id.starts_with("\"") {
                                                    str_id =
                                                        str_id[1..][..str_id.len() - 2].to_string();
                                                }
                                                str_id
                                            }
                                            sqlparser::sqlast::ASTNode::SQLValue(
                                                ref right_value,
                                            ) => match right_value {
                                                sqlparser::sqlast::Value::SingleQuotedString(s) => {
                                                    s.to_string()
                                                }
                                                _ => right_value.to_string(),
                                            },
                                            _ => "".to_string(),
                                        };
                                        // TODO: Add support for wildcards ie: LIKE 'server_.domain.com' where _ is a single character wildcard
                                        if identifier == "$line" {
                                            if line.contains(&op_value[..]) == false {
                                                all_conditions_pass = false;
                                            }
                                        } else {
                                            if projection_values.contains_key(&identifier[..])
                                                && projection_values[&identifier]
                                                    .contains(&op_value[..])
                                                    == false
                                            {
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
    return skip_line;
}

/// Extracts an `ASTNode` identifier as a `String`
pub fn get_identifier_from_ast(ast: &ASTNode) -> String {
    match ast {
        sqlparser::sqlast::ASTNode::SQLIdentifier(ref identifier) => identifier.to_string(),
        _ => {
            // TODO: Should we be retunring anything at all?
            "".to_string()
        }
    }
}

#[cfg(test)]
mod filter_tests {
    use super::*;

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
}
