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

import { ActionReducerMap, createFeatureSelector } from '@ngrx/store';

import { AppState } from '../../core/core.module';


export const FEATURE_NAME = 'examples';
export const selectExamples = createFeatureSelector<State, ConfigState>(
  FEATURE_NAME
);
export const reducers: ActionReducerMap<ConfigState> = {
};

export interface ConfigState {
}

export interface State extends AppState {
  examples: ConfigState;
}
