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

use minsql::{Bootstrap, MinSQL};
use std::sync::Arc;

fn main() {
    pretty_env_logger::init();

    // Load configuration
    Bootstrap::load_config();
    let sing_config = Bootstrap::get_cfg();
    let cfg = Arc::clone(&sing_config);

    // Start minSQL
    let minsql_c = MinSQL::new(cfg);
    minsql_c.run();
}
