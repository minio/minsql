// MinSQL
// Copyright (C) 2019  MinIO
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

use regex::Regex;
use crate::constants::SF_IP;
use crate::constants::SF_EMAIL;
use crate::constants::SF_DATE;

bitflags! {
    pub struct ScanFlags: u32 {
        const NONE = 0b00000000;
        const IP = 0b00000001;
        const EMAIL = 0b00000010;
        const DATE = 0b00000100;
    }
}


pub fn scanlog(text: &String, flags: ScanFlags) -> HashMap<String, Vec<String>> {
    // Compile the regex only once
    lazy_static! {
        static ref IP_RE :Regex= Regex::new(r"(((25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9][0-9]|[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9][0-9]|[0-9]))").unwrap();
        static ref EMAIL_RE :Regex= Regex::new(r"([\w\.!#$%&'*+\-=?\^_`{|}~]+@([\w\d-]+\.)+[\w]{2,4})").unwrap();
        // TODO: This regex matches a fairly simple date format, improve : 2019-05-23
        static ref DATE_RE :Regex= Regex::new(r"(19[789]\d|2\d{3})-((0[13578]|1[02])-([0-2]\d|3[01])|02-[0-2]\d|0[469]|11-([0-2]\d|30))").unwrap();
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
    results
}