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

import {DataSource} from '@angular/cdk/table';

import {BehaviorSubject, Observable, of} from 'rxjs';

import {CollectionViewer} from '@angular/cdk/collections';
import {catchError, finalize} from 'rxjs/operators';
import {DataStore, Log, Token} from './structs';
import {ApiService} from '../core/api-service/api.service';

export class LogsDataSource implements DataSource<Log> {

  private logsSubject = new BehaviorSubject<Log[]>([]);
  private totalSubject = new BehaviorSubject<number>(0);
  private loadingSubject = new BehaviorSubject<boolean>(false);

  public loading$ = this.loadingSubject.asObservable();


  constructor(private apiService: ApiService) {
  }

  connect(collectionViewer: CollectionViewer): Observable<Log[] | ReadonlyArray<Log>> {
    return this.logsSubject.asObservable();
  }

  disconnect(collectionViewer: CollectionViewer): void {
    this.logsSubject.complete();
    this.loadingSubject.complete();
  }

  totalElements() {
    return this.totalSubject.asObservable();
  }

  loadElements(page?: number, pageSize?: number) {
    if (page == null) {
      page = 0;
    }
    if (pageSize == null) {
      pageSize = 10;
    }
    this.loadingSubject.next(true);
    const offset = page * pageSize;
    this.apiService.get(`/api/logs?offset=${offset}&limit=${pageSize}`).pipe(
      catchError(() => of([])),
      finalize(() => this.loadingSubject.next(false))
    ).subscribe(logs => {
      this.logsSubject.next(logs['results']);
      this.totalSubject.next(logs['total']);
    });
  }

}


export class DatastoresDataSource implements DataSource<DataStore> {

  private datastoresSubject = new BehaviorSubject<DataStore[]>([]);
  private totalSubject = new BehaviorSubject<number>(0);
  private loadingSubject = new BehaviorSubject<boolean>(false);

  public loading$ = this.loadingSubject.asObservable();


  constructor(private apiService: ApiService) {
  }

  connect(collectionViewer: CollectionViewer): Observable<DataStore[] | ReadonlyArray<DataStore>> {
    return this.datastoresSubject.asObservable();
  }

  disconnect(collectionViewer: CollectionViewer): void {
    this.datastoresSubject.complete();
    this.loadingSubject.complete();
  }

  totalElements() {
    return this.totalSubject.asObservable();
  }

  loadElements(page?: number, pageSize?: number) {
    if (page == null) {
      page = 0;
    }
    if (pageSize == null) {
      pageSize = 10;
    }
    this.loadingSubject.next(true);
    const offset = page * pageSize;
    this.apiService.get(`/api/datastores?offset=${offset}&limit=${pageSize}`).pipe(
      catchError(() => of([])),
      finalize(() => this.loadingSubject.next(false))
    ).subscribe(logs => {
      this.datastoresSubject.next(logs['results']);
      this.totalSubject.next(logs['total']);
    });
  }

}

export class TokensDataSource implements DataSource<Token> {

  private tokensSubject = new BehaviorSubject<Token[]>([]);
  private totalSubject = new BehaviorSubject<number>(0);
  private loadingSubject = new BehaviorSubject<boolean>(false);

  public loading$ = this.loadingSubject.asObservable();


  constructor(private apiService: ApiService) {
  }

  connect(collectionViewer: CollectionViewer): Observable<Token[] | ReadonlyArray<Token>> {
    return this.tokensSubject.asObservable();
  }

  disconnect(collectionViewer: CollectionViewer): void {
    this.tokensSubject.complete();
    this.loadingSubject.complete();
  }

  totalElements() {
    return this.totalSubject.asObservable();
  }

  loadElements(page?: number, pageSize?: number) {
    if (page == null) {
      page = 0;
    }
    if (pageSize == null) {
      pageSize = 10;
    }
    this.loadingSubject.next(true);
    const offset = page * pageSize;
    this.apiService.get(`/api/tokens?offset=${offset}&limit=${pageSize}`).pipe(
      catchError(() => of([])),
      finalize(() => this.loadingSubject.next(false))
    ).subscribe(logs => {
      this.tokensSubject.next(logs['results']);
      this.totalSubject.next(logs['total']);
    });
  }

}
