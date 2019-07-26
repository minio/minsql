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

import {CommonModule} from '@angular/common';
import {ErrorHandler, NgModule, Optional, SkipSelf} from '@angular/core';
import {HTTP_INTERCEPTORS, HttpClient, HttpClientModule} from '@angular/common/http';
import {RouterStateSerializer, StoreRouterConnectingModule} from '@ngrx/router-store';
import {StoreModule} from '@ngrx/store';
import {EffectsModule} from '@ngrx/effects';
import {StoreDevtoolsModule} from '@ngrx/store-devtools';
import {TranslateLoader, TranslateModule} from '@ngx-translate/core';
import {TranslateHttpLoader} from '@ngx-translate/http-loader';

import {environment} from '../../environments/environment';

import {AppState, metaReducers, reducers, selectRouterState} from './core.state';
import {AuthEffects} from './auth/auth.effects';
import {selectAuth, selectIsAuthenticated} from './auth/auth.selectors';
import {authLogin, authLogout} from './auth/auth.actions';
import {AuthGuardService} from './auth/auth-guard.service';
import {TitleService} from './title/title.service';
import {ROUTE_ANIMATIONS_ELEMENTS, routeAnimations} from './animations/route.animations';
import {AnimationsService} from './animations/animations.service';
import {AppErrorHandler} from './error-handler/app-error-handler.service';
import {CustomSerializer} from './router/custom-serializer';
import {LocalStorageService} from './local-storage/local-storage.service';
import {HttpErrorInterceptor} from './http-interceptors/http-error.interceptor';
import {NotificationService} from './notifications/notification.service';
import {SettingsEffects} from './settings/settings.effects';
import {selectEffectiveTheme, selectSettingsLanguage, selectSettingsStickyHeader} from './settings/settings.selectors';
import {ApiService} from './api-service/api.service';
import {AppStateService} from './app-state/app-state.service';

export {
  TitleService,
  selectAuth,
  authLogin,
  authLogout,
  routeAnimations,
  AppState,
  LocalStorageService,
  selectIsAuthenticated,
  ROUTE_ANIMATIONS_ELEMENTS,
  AnimationsService,
  AuthGuardService,
  selectRouterState,
  NotificationService,
  selectEffectiveTheme,
  selectSettingsLanguage,
  selectSettingsStickyHeader
};

export function HttpLoaderFactory(http: HttpClient) {
  return new TranslateHttpLoader(
    http,
    `${environment.i18nPrefix}/assets/i18n/`,
    '.json'
  );
}

@NgModule({
  imports: [
    // angular
    CommonModule,
    HttpClientModule,

    // ngrx
    StoreModule.forRoot(reducers, {metaReducers}),
    StoreRouterConnectingModule.forRoot(),
    EffectsModule.forRoot([
      AuthEffects,
      SettingsEffects,
    ]),
    environment.production
      ? []
      : StoreDevtoolsModule.instrument({
        name: 'MinSQL'
      }),

    // 3rd party
    TranslateModule.forRoot({
      loader: {
        provide: TranslateLoader,
        useFactory: HttpLoaderFactory,
        deps: [HttpClient]
      }
    })
  ],
  declarations: [],
  providers: [
    {provide: HTTP_INTERCEPTORS, useClass: HttpErrorInterceptor, multi: true},
    {provide: ErrorHandler, useClass: AppErrorHandler},
    {provide: RouterStateSerializer, useClass: CustomSerializer},
    ApiService,
    AppStateService
  ],
  exports: [TranslateModule]
})
export class CoreModule {
  constructor(
    @Optional()
    @SkipSelf()
      parentModule: CoreModule
  ) {
    if (parentModule) {
      throw new Error('CoreModule is already loaded. Import only in AppModule');
    }
  }
}
