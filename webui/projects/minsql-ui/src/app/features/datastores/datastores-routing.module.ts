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

import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { DatastoresComponent } from './datastores/datastores.component';
import {DatastoreDetailsComponent} from './datastore-details/datastore-details.component';
import {DatastoresListComponent} from './datastores-list/datastores-list.component';

const routes: Routes = [
  {
    path: '',
    component: DatastoresComponent,
    children: [
      {
        path: ':name',
        component: DatastoreDetailsComponent,
        data: {title: 'minsql.datastores.title'}
      },
      {
        path: '',
        component: DatastoresListComponent,
        data: {title: 'minsql.datastores.title'}
      },
    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class DatastoresRoutingModule {}
