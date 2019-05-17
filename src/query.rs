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
use hyperscan::*;
use std::time::Instant;

fn callback(id: u32, from: u64, to: u64, flags: u32, _: &BlockDatabase) -> u32 {
    assert_eq!(id, 0);
    assert_eq!(from, 5);
    assert_eq!(to, 9);
    assert_eq!(flags, 0);

    println!("found pattern #{} @ [{}, {})", id, from, to);

    0
}

pub fn scanlog() {
    let start = Instant::now();
    let pattern = &pattern! {"test", flags => HS_FLAG_CASELESS|HS_FLAG_SOM_LEFTMOST};
    let db: BlockDatabase = pattern.build().unwrap();
    let scratch = db.alloc().unwrap();

    db.scan::<BlockDatabase>("some test data", 0, &scratch, Some(callback), Some(&db)).unwrap();
    let duration = start.elapsed();
    println!("Scanning string: {:?}", duration);
}