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

import * as assert from 'assert';
import { ActivationEnd } from '@angular/router';
import { Actions, getEffectsMetadata } from '@ngrx/effects';
import { TranslateService } from '@ngx-translate/core';
import { Store } from '@ngrx/store';
import { TestScheduler } from 'rxjs/testing';

import { TitleService } from '../../core/core.module';

import { actionSettingsChangeLanguage } from '../../core/settings/settings.actions';

import { ConfigEffects } from './config.effects';
import { State } from './config.state';

const scheduler = new TestScheduler((actual, expected) =>
  assert.deepStrictEqual(actual, expected)
);

describe('SettingsEffects', () => {
  let router: any;
  let titleService: jasmine.SpyObj<TitleService>;
  let translateService: jasmine.SpyObj<TranslateService>;
  let store: jasmine.SpyObj<Store<State>>;

  beforeEach(() => {
    router = {
      routerState: {
        snapshot: {
          root: {}
        }
      },
      events: {
        pipe() {}
      }
    };

    titleService = jasmine.createSpyObj('TitleService', ['setTitle']);
    translateService = jasmine.createSpyObj('TranslateService', ['use']);
    store = jasmine.createSpyObj('store', ['pipe']);
  });

  describe('setTranslateServiceLanguage', () => {
    it('should not dispatch action', () => {
      const actions = new Actions<any>();
      const effect = new ConfigEffects(
        actions,
        store,
        translateService,
        router,
        titleService
      );
      const metadata = getEffectsMetadata(effect);
      expect(metadata.setTranslateServiceLanguage.dispatch).toEqual(false);
    });
  });

  describe('setTitle', () => {
    it('should not dispatch action', () => {
      const actions = new Actions<any>();
      const effect = new ConfigEffects(
        actions,
        store,
        translateService,
        router,
        titleService
      );
      const metadata = getEffectsMetadata(effect);

      expect(metadata.setTitle.dispatch).toEqual(false);
    });

    it('should setTitle', () => {
      scheduler.run(helpers => {
        const { cold, hot } = helpers;
        const action = actionSettingsChangeLanguage({ language: 'en' });
        const actions = hot('-a', { a: action });

        const routerEvent = new ActivationEnd(router.routerState.snapshot);
        router.events = cold('a', { a: routerEvent });

        const effect = new ConfigEffects(
          actions,
          store,
          translateService,
          router,
          titleService
        );

        effect.setTitle.subscribe(() => {
          expect(titleService.setTitle).toHaveBeenCalled();
          expect(titleService.setTitle).toHaveBeenCalledWith(
            router.routerState.snapshot.root,
            translateService
          );
        });
      });
    });
  });
});
