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

import {ChangeDetectionStrategy, ChangeDetectorRef, Component, OnInit} from '@angular/core';
import {ActivatedRoute, Router} from '@angular/router';
import {MatDialog} from '@angular/material';
import {HttpErrorResponse} from '@angular/common/http';
import {DataStore} from '../../../shared/structs';
import {DatastoresDataSource} from '../../../shared/datasources';
import {ApiService} from '../../../core/api-service/api.service';
import {NotificationService, ROUTE_ANIMATIONS_ELEMENTS, routeAnimations} from '../../../core/core.module';
import {ConfirmPromptComponent} from '../../../shared/confirm-prompt/confirm-prompt.component';

@Component({
  selector: 'minsql-datastore-details',
  templateUrl: './datastore-details.component.html',
  styleUrls: ['./datastore-details.component.scss'],
  animations: [routeAnimations],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class DatastoreDetailsComponent implements OnInit {
  routeAnimationsElements = ROUTE_ANIMATIONS_ELEMENTS;

  datastoreName: String;
  datastore: DataStore = null;
  addingNew: Boolean = false;
  queryParamSub: any;
  dataStoresDataSource: DatastoresDataSource;

  constructor(
    public diaDataStore: MatDialog,
    private apiService: ApiService,
    private notificationsService: NotificationService,
    private router: Router,
    private cd: ChangeDetectorRef,
    private activatedRoute: ActivatedRoute) {
    this.dataStoresDataSource = new DatastoresDataSource(this.apiService);
  }

  ngOnInit() {
    this.queryParamSub = this.activatedRoute.params.subscribe((params) => {
      this.datastoreName = params['name'];
      if (this.datastoreName !== 'new') {
        this.fetchRecord();
      } else {
        this.addingNew = true;
        this.datastore = new DataStore();
      }
    });
    this.dataStoresDataSource.loadElements();
  }

  fetchRecord() {
    this.apiService.get(`/api/datastores/${this.datastoreName}`).subscribe((response: DataStore) => {
      this.datastore = response;
      // default to empty
      this.datastore.secret_key = '';
      this.cd.detectChanges();
    })
  }


  save() {
    if (this.addingNew) {
      this.apiService.post(`/api/datastores`, this.datastore).subscribe(
        (response) => this.successCallback(response),
        (error) => this.errorCallback(error));
    } else {
      this.apiService.put(`/api/datastores/${this.datastore.name}`, this.datastore).subscribe(
        (response) => this.successCallback(response),
        (error) => this.errorCallback(error));
    }
  }

  delete() {
    const diaDataStoreRef = this.diaDataStore.open(ConfirmPromptComponent, {
      width: '400px',
      data: {
        title: 'Delete DataStore',
        message: `Are you sure you want to delete DataStore \`${this.datastoreName}\``
      }
    });

    diaDataStoreRef.afterClosed().subscribe(result => {
      if (result) {
        this.doDelete();
      }
    });
  }

  doDelete() {
    this.apiService.delete(`/api/datastores/${this.datastoreName}`).subscribe(
      (response) => this.successCallback(response),
      (error) => this.errorCallback(error));
  }

  successCallback(response) {
    this.router.navigate(['configuration', 'datastores']);
  }

  errorCallback(error: HttpErrorResponse) {
    const errorMessage = error.error['message'].replace('Bad request: ', '');
    this.notificationsService.error(`Error saving: ${errorMessage}`);
  }

}
