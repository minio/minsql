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

import {MatDialog, MatPaginator} from '@angular/material';
import {tap} from 'rxjs/operators';
import {AfterViewInit, ChangeDetectionStrategy, ChangeDetectorRef, Component, OnInit, ViewChild} from '@angular/core';
import {ActivatedRoute, Router} from '@angular/router';
import {HttpErrorResponse} from '@angular/common/http';
import {Log, LogAuth, Token} from '../../../shared/structs';
import {LogsDataSource} from '../../../shared/datasources';
import {ApiService} from '../../../core/api-service/api.service';
import {NotificationService} from '../../../core/core.module';
import {ConfirmPromptComponent} from '../../../shared/confirm-prompt/confirm-prompt.component';

@Component({
  selector: 'minsql-token-details',
  templateUrl: './token-details.component.html',
  styleUrls: ['./token-details.component.scss'],
  changeDetection: ChangeDetectionStrategy.Default
})
export class TokenDetailsComponent implements OnInit, AfterViewInit {

  tokenName: String;
  token: Token = null;
  addingNew: Boolean = false;
  queryParamSub: any;

  currentLogListAuth: { [logName: string]: LogAuth } = {};
  changesLogAuth: { [logName: string]: string[] } = {};


  logDisplayedColumns: string[] = ['name', 'search', 'store'];
  logsDataSource: LogsDataSource;

  @ViewChild(MatPaginator, {static: false}) paginator: MatPaginator;

  constructor(
    public diaDataStore: MatDialog,
    private apiService: ApiService,
    private notificationsService: NotificationService,
    private router: Router,
    private cd: ChangeDetectorRef,
    private activatedRoute: ActivatedRoute) {
    this.logsDataSource = new LogsDataSource(this.apiService);
  }

  ngOnInit() {
    this.queryParamSub = this.activatedRoute.params.subscribe((params) => {
      this.tokenName = params['access_key'];
      if (this.tokenName !== 'new') {
        this.fetchRecord();
      } else {
        this.addingNew = true;
        this.token = new Token();
      }
    });
    this.logsDataSource.loadElements();
    // Since we don't have a filter to request multiple records at the same time
    // we are going to ask for the authorization for every token we are displaying on screen
    this.logsDataSource.connect(null).subscribe((data: Log[]) => {
      this.currentLogListAuth = {};
      for (const datum of data) {
        this.apiService.get(`/api/auth/${this.tokenName}/${datum.name}`).subscribe((auth: LogAuth) => {
          this.currentLogListAuth[auth.log_name] = auth;
          this.cd.detectChanges();
        })
      }
    });

  }

  ngAfterViewInit() {
    this.paginator.page
      .pipe(
        tap(() => this.loadLogs())
      )
      .subscribe();
  }

  loadLogs() {
    this.logsDataSource.loadElements(
      this.paginator.pageIndex,
      this.paginator.pageSize
    );
  }

  fetchRecord() {
    this.apiService.get(`/api/tokens/${this.tokenName}`).subscribe((response: Token) => {
      this.token = response;
      // default to empty
      this.token.secret_key = '';
    })
  }


  save() {
    if (this.addingNew) {
      this.apiService.post(`/api/tokens`, this.token).subscribe(
        (response) => {

          for (const logName in this.changesLogAuth) {
            this.apiService.post(`/api/auth/${this.tokenName}`, {
              'log_name': logName,
              'api': this.changesLogAuth[logName],
            }).subscribe((_) => {
            })
          }

          this.successCallback(response);
        },
        (error) => this.errorCallback(error));
    } else {
      // remove immutable fields
      const output = {};
      for (const key in this.token) {
        output[key] = this.token[key];
      }
      delete output['access_key'];
      delete output['secret_key'];
      this.apiService.put(`/api/tokens/${this.token.access_key}`, output).subscribe(
        (response) => {

          for (const logName in this.changesLogAuth) {
            this.apiService.post(`/api/auth/${this.tokenName}`, {
              'log_name': logName,
              'api': this.changesLogAuth[logName],
            }).subscribe((_) => {
            })
          }

          this.successCallback(response);
        },
        (error) => this.errorCallback(error));
    }
  }

  delete() {
    const diaDataStoreRef = this.diaDataStore.open(ConfirmPromptComponent, {
      width: '400px',
      data: {
        title: 'Delete DataStore',
        message: `Are you sure you want to delete DataStore \`${this.tokenName}\``
      }
    });

    diaDataStoreRef.afterClosed().subscribe(result => {
      if (result) {
        this.doDelete();
      }
    });
  }

  doDelete() {
    this.apiService.delete(`/api/tokens/${this.tokenName}`).subscribe(
      (response) => this.successCallback(response),
      (error) => this.errorCallback(error));
  }

  successCallback(response) {
    this.router.navigate(['configuration', 'tokens']);
  }

  errorCallback(error: HttpErrorResponse) {
    const errorMessage = error.error['message'].replace('Bad request: ', '');
    this.notificationsService.error(`Error saving: ${errorMessage}`);
  }

  isChecked(log: Log, api: string): boolean {
    if ((this.currentLogListAuth.hasOwnProperty(log.name) && this.currentLogListAuth[log.name].api.includes(api)) ||
      (this.changesLogAuth.hasOwnProperty(log.name) && this.changesLogAuth[log.name].includes(api))) {
      return true;
    }
    return false;
  }

  // flips the state of an authorization for a log
  flippedLogCB(log: Log, api: string) {
    if (this.changesLogAuth.hasOwnProperty(log.name) === false) {
      this.changesLogAuth[log.name] = [api];
    } else {
      if (this.changesLogAuth[log.name].includes(api)) {
        this.changesLogAuth[log.name].splice(this.changesLogAuth[log.name].indexOf(api), 1);
      } else {
        this.changesLogAuth[log.name].push(api);
      }
    }
  }

}
