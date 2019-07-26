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

import {Injectable} from '@angular/core';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {isNullOrUndefined} from 'util';

@Injectable({
  providedIn: 'root'
})
export class ApiService {

  constructor(private http: HttpClient) {
  }

  get(url) {
    const httpOptions = this.getHeaders();
    return this.http.get(url, httpOptions);
  }

  post(url, data, options?) {
    let headers = {};
    if (!isNullOrUndefined(options)) {
      if (options.hasOwnProperty('headers')) {
        headers = options['headers'];
      }
    }
    const httpOptions = this.getHeaders(headers);
    for (const key in options) {
      if (options.hasOwnProperty(key) && key !== 'headers') {
        httpOptions[key] = options[key];
      }
    }
    return this.http.post(url, data, httpOptions);
  }

  put(url, data) {
    const httpOptions = this.getHeaders();
    return this.http.put(url, data, httpOptions);
  }

  delete(url) {
    const httpOptions = this.getHeaders();
    return this.http.delete(url, httpOptions);
  }

  private getHeaders(extraHeaders?: object) {
    const accessKey = localStorage.getItem('access_key');
    const secretKey = localStorage.getItem('secret_key');

    const headers = {
      'MINSQL-TOKEN': `${accessKey}${secretKey}`
    };
    for (const key in extraHeaders) {
      if (extraHeaders.hasOwnProperty(key)) {
        headers[key] = extraHeaders[key];
      }
    }

    const httpOptions = {
      headers: new HttpHeaders(headers)
    };
    return httpOptions;
  }
}
