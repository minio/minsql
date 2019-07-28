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
import { CommonModule } from '@angular/common';



import { LoginComponent } from './login/login.component';
import { LoginRoutingModule } from './login-routing.module';
import {SharedModule} from '../shared/shared.module';
import {MatCardModule} from '@angular/material';

@NgModule({
  declarations: [LoginComponent],
  imports: [CommonModule, SharedModule, LoginRoutingModule, MatCardModule]
})
export class LoginModule {}
