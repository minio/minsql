// This file is part of the MinSQL
// Copyright (c) 2019 MinIO, Inc.s
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
use sqlparser::dialect::Dialect;

#[derive(Debug)]
pub struct MinSQLDialect {}

impl Dialect for MinSQLDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // our identifiers can start with @ or $
        (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '@' || ch == '$'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z')
            || (ch >= 'A' && ch <= 'Z')
            || (ch >= '0' && ch <= '9')
            || ch == '_'
    }
}
