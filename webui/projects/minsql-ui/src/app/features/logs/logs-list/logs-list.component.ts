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

import {AfterViewInit, ChangeDetectionStrategy, Component, OnInit, ViewChild} from '@angular/core';
import {MatPaginator} from '@angular/material';

import {Router} from '@angular/router';


import {tap} from 'rxjs/operators';
import {ROUTE_ANIMATIONS_ELEMENTS, routeAnimations} from '../../../core/core.module';
import {Log} from '../../../shared/structs';
import {LogsDataSource} from '../../../shared/datasources';
import {ApiService} from '../../../core/api-service/api.service';


@Component({
  selector: 'minsql-logs-list',
  templateUrl: './logs-list.component.html',
  styleUrls: ['./logs-list.component.scss'],
  animations: [routeAnimations],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class LogsListComponent implements OnInit, AfterViewInit {
  routeAnimationsElements = ROUTE_ANIMATIONS_ELEMENTS;
  displayedColumns: string[] = ['name', 'commit_window', 'datastores'];
  data: Log[];
  dataSource: LogsDataSource;

  @ViewChild(MatPaginator, {static: false}) paginator: MatPaginator;

  constructor(public apiService: ApiService, public router: Router) {
  }

  ngOnInit() {
    this.dataSource = new LogsDataSource(this.apiService);
    this.dataSource.loadElements();
  }

  ngAfterViewInit() {
    this.paginator.page
      .pipe(
        tap(() => this.loadData())
      )
      .subscribe();
  }

  loadData() {
    this.dataSource.loadElements(
      this.paginator.pageIndex,
      this.paginator.pageSize
    );
  }

  clickOn(row: Log) {
    this.router.navigate(['configuration', 'logs', row.name]);
  }

  add() {
    this.router.navigate(['configuration', 'logs', 'new']);
  }

}

