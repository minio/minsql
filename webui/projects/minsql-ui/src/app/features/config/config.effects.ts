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

import { Injectable } from '@angular/core';
import { ActivationEnd, Router } from '@angular/router';
import { TranslateService } from '@ngx-translate/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { Store, select } from '@ngrx/store';
import { merge } from 'rxjs';
import { tap, distinctUntilChanged, filter } from 'rxjs/operators';

import {
  TitleService,
  AppState,
  selectSettingsLanguage
} from '../../core/core.module';
import { actionSettingsChangeLanguage } from '../../core/settings/settings.actions';

@Injectable()
export class ConfigEffects {
  constructor(
    private actions$: Actions,
    private store: Store<AppState>,
    private translateService: TranslateService,
    private router: Router,
    private titleService: TitleService
  ) {}

  setTranslateServiceLanguage = createEffect(
    () => () =>
      this.store.pipe(
        select(selectSettingsLanguage),
        distinctUntilChanged(),
        tap(language => this.translateService.use(language))
      ),
    { dispatch: false }
  );

  setTitle = createEffect(
    () =>
      merge(
        this.actions$.pipe(ofType(actionSettingsChangeLanguage)),
        this.router.events.pipe(filter(event => event instanceof ActivationEnd))
      ).pipe(
        tap(() => {
          this.titleService.setTitle(
            this.router.routerState.snapshot.root,
            this.translateService
          );
        })
      ),
    { dispatch: false }
  );
}
