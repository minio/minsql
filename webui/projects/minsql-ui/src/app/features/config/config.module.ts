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
import { HttpClient } from '@angular/common/http';
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { TranslateModule, TranslateLoader } from '@ngx-translate/core';
import { TranslateHttpLoader } from '@ngx-translate/http-loader';

import { SharedModule } from '../../shared/shared.module';
import { environment } from '../../../environments/environment';

import { FEATURE_NAME, reducers } from './config.state';
import { ConfigRoutingModule } from './config-routing.module';
import { ConfigComponent } from './config/config.component';
import { ConfigEffects } from './config.effects';
import {UserComponent} from './simple-state-management/components/user.component';

export function HttpLoaderFactory(http: HttpClient) {
  return new TranslateHttpLoader(
    http,
    `${environment.i18nPrefix}/assets/i18n/config/`,
    '.json'
  );
}

@NgModule({
  imports: [
    SharedModule,
    ConfigRoutingModule,
    StoreModule.forFeature(FEATURE_NAME, reducers),
    TranslateModule.forChild({
      loader: {
        provide: TranslateLoader,
        useFactory: HttpLoaderFactory,
        deps: [HttpClient]
      },
      isolate: true
    }),
    EffectsModule.forFeature([
      ConfigEffects,
    ])
  ],
  declarations: [
    ConfigComponent,
    UserComponent,
  ],
  providers: []
})
export class ConfigModule {
  constructor() {}
}
