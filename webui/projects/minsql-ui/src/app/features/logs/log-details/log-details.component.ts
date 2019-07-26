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

import {AfterViewInit, ChangeDetectionStrategy, ChangeDetectorRef, Component, OnInit, ViewChild} from '@angular/core';

import {ActivatedRoute, Router} from '@angular/router';

import {MatCheckboxChange, MatDialog, MatPaginator} from '@angular/material';

import {HttpErrorResponse} from '@angular/common/http';
import {NotificationService, ROUTE_ANIMATIONS_ELEMENTS, routeAnimations} from '../../../core/core.module';
import {ApiService} from '../../../core/api-service/api.service';
import {DatastoresDataSource} from '../../../shared/datasources';
import {DataStore, Log} from '../../../shared/structs';
import {ConfirmPromptComponent} from '../../../shared/confirm-prompt/confirm-prompt.component';
import {tap} from 'rxjs/operators';


@Component({
  selector: 'minsql-log-details',
  templateUrl: './log-details.component.html',
  styleUrls: ['./log-details.component.scss'],
  animations: [routeAnimations],
  changeDetection: ChangeDetectionStrategy.Default
})
export class LogDetailsComponent implements OnInit, AfterViewInit {
  routeAnimationsElements = ROUTE_ANIMATIONS_ELEMENTS;
  displayedColumns = ['name', 'bucket', 'use'];

  logName: String;
  log: Log = null;
  addingNew: Boolean = false;
  queryParamSub: any;
  dataStoresDataSource: DatastoresDataSource;

  constructor(
    public dialog: MatDialog,
    private apiService: ApiService,
    private notificationsService: NotificationService,
    private router: Router,
    private cd: ChangeDetectorRef,
    private activatedRoute: ActivatedRoute) {
    this.dataStoresDataSource = new DatastoresDataSource(this.apiService);
  }

  @ViewChild(MatPaginator, {static: false}) paginator: MatPaginator;

  ngOnInit() {
    this.queryParamSub = this.activatedRoute.params.subscribe((params) => {
      this.logName = params['name'];
      if (this.logName !== 'new') {
        this.fetchRecord();
      } else {
        this.addingNew = true;
        this.log = new Log();
        // Default
        this.log.commit_window = '5s';
      }
    });
  }


  ngAfterViewInit(): void {
    this.paginator.page
      .pipe(
        tap(() => this.loadDatastores())
      )
      .subscribe();

    this.loadDatastores();
  }

  loadDatastores(){
    this.dataStoresDataSource.loadElements(
      this.paginator.pageIndex,
      this.paginator.pageSize
    );
  }

  fetchRecord() {
    this.apiService.get(`/api/logs/${this.logName}`).subscribe((response: Log) => {
      this.log = response;
      this.cd.detectChanges();
    })
  }

  datastoreChanged(datastore: DataStore, cbox: MatCheckboxChange) {
    if (cbox.checked) {
      this.log.datastores.push(datastore.name);
    } else {
      const elementIndex = this.log.datastores.indexOf(datastore.name, 0);
      this.log.datastores.splice(elementIndex, 1);
    }
  }

  toggleCheckbox(datastore: DataStore) {
    const checked = this.log.datastores.includes(datastore.name);
    if (!checked) {
      this.log.datastores.push(datastore.name);
    } else {
      const elementIndex = this.log.datastores.indexOf(datastore.name, 0);
      this.log.datastores.splice(elementIndex, 1);
    }
  }

  save() {
    if (this.log.datastores.length === 0) {
      this.notificationsService.error('At least one datastore must be used.');
      return;
    }
    if (this.addingNew) {
      this.apiService.post(`/api/logs`, this.log).subscribe(
        (response) => this.successCallback(response),
        (error) => this.errorCallback(error));
    } else {
      this.apiService.put(`/api/logs/${this.log.name}`, this.log).subscribe(
        (response) => this.successCallback(response),
        (error) => this.errorCallback(error));
    }
  }

  delete() {
    const dialogRef = this.dialog.open(ConfirmPromptComponent, {
      width: '400px',
      data: {
        title: 'Delete Log',
        message: `Are you sure you want to delete log \`${this.logName}\``
      }
    });

    dialogRef.afterClosed().subscribe(result => {
      if (result) {
        this.doDelete();
      }
    });
  }

  doDelete() {
    this.apiService.delete(`/api/logs/${this.logName}`).subscribe(
      (response) => this.successCallback(response),
      (error) => this.errorCallback(error));
  }

  successCallback(response) {
    this.router.navigate(['configuration',  'logs']);
  }

  errorCallback(error: HttpErrorResponse) {
    const errorMessage = error.error['message'].replace('Bad request: ', '');
    this.notificationsService.error(`Error saving: ${errorMessage}`);
  }

}
