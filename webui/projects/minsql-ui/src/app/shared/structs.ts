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


export class Log {
  name: string;
  datastores: string[];
  commit_window: string;

  constructor() {
    this.name = '';
    this.commit_window = '';
    this.datastores = [];
  }
}


export class DataStore {
  name: string;
  endpoint: string;
  access_key: string;
  secret_key: string;
  bucket: string;
  prefix = '';
}

export class Token {
  access_key: string;
  secret_key: string;
  description: string;
  is_admin = false;
  enabled = true;
}

export class LogAuth {
  log_name: string;
  api: string[];
  expire: string;
  status: string;
}
