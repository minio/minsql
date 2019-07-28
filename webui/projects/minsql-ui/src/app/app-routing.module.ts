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
import { Routes, RouterModule, PreloadAllModules } from '@angular/router';
import {AuthGuardService} from './core/core.module';

const routes: Routes = [
  {
    path: '',
    redirectTo: 'query',
    pathMatch: 'full'
  },
  {
    path: 'query',
    canActivate: [AuthGuardService],
    loadChildren: () =>
      import('./features/query/query.module').then(m => m.QueryModule)
  },
  {
    path: 'configuration',
    canActivate: [AuthGuardService],
    loadChildren: () =>
      import('./features/config/config.module').then(m => m.ConfigModule)
  },
  {
    path: 'login',
    loadChildren: () =>
      import('./login/login.module').then(m => m.LoginModule)
  },
  {
    path: '**',
    redirectTo: 'query'
  }
];

@NgModule({
  // useHash supports github.io demo page, remove in your app
  imports: [
    RouterModule.forRoot(routes, {
      useHash: false,
      scrollPositionRestoration: 'enabled',
      preloadingStrategy: PreloadAllModules
    })
  ],
  exports: [RouterModule]
})
export class AppRoutingModule {}
