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

import { Component, OnInit, ChangeDetectionStrategy } from '@angular/core';
import {NotificationService, ROUTE_ANIMATIONS_ELEMENTS, routeAnimations} from '../../core/core.module';
import {Router} from '@angular/router';
import {HttpClient, HttpHeaders} from '@angular/common/http';



@Component({
  selector: 'minsql-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.scss'],
  animations: [routeAnimations],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class LoginComponent implements OnInit {
  routeAnimationsElements = ROUTE_ANIMATIONS_ELEMENTS;
  accessKey: string;
  secretKey: string;

  constructor(private http: HttpClient,
              private router: Router,
              private notificationsService: NotificationService) {}

  ngOnInit() {}

  doLogin() {
    const httpOptions = {
      headers: new HttpHeaders({
        'MINSQL-TOKEN': `${this.accessKey}${this.secretKey}`
      })
    };

    this.http.get(`/api/tokens/${this.accessKey}`, httpOptions).subscribe((resp) => {
      localStorage.setItem('access_key', this.accessKey);
      localStorage.setItem('secret_key', this.secretKey);
      this.router.navigate(['/']);

    }, (error) => {
      this.notificationsService.error('Invalid credentials.');
    });
  }

}
