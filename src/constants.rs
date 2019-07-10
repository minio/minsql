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

// Server Defaults
pub const DEFAULT_SERVER_ADDRESS: &str = "0.0.0.0:9999";
// environment variables
pub const METABUCKET_ENDPOINT: &str = "MINSQL_METABUCKET_ENDPOINT";
pub const METABUCKET_NAME: &str = "MINSQL_METABUCKET_NAME";
pub const METABUCKET_ACCESS_KEY: &str = "MINSQL_METABUCKET_ACCESS_KEY";
pub const PKCS12_CERT: &str = "MINSQL_PKCS12_CERT";
pub const PKCS12_PASSWORD: &str = "MINSQL_PKCS12_PASSWORD";

// Smart Fields
pub const SF_IP: &str = "$ip";
pub const SF_EMAIL: &str = "$email";
pub const SF_DATE: &str = "$date";
pub const SF_QUOTED: &str = "$quoted";
pub const SF_URL: &str = "$url";
