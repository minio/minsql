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

import { Component, OnInit, ChangeDetectionStrategy } from '@angular/core';
import {
  FormBuilder,
  FormGroup,
  FormGroupDirective,
  Validators
} from '@angular/forms';
import { Observable } from 'rxjs';
import { map, startWith } from 'rxjs/operators';

import { ROUTE_ANIMATIONS_ELEMENTS } from '../../../../core/animations/route.animations';

import { User, UserService } from '../user.service';

@Component({
  selector: 'minsql-user',
  templateUrl: './user.component.html',
  styleUrls: ['./user.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class UserComponent implements OnInit {
  routeAnimationsElements = ROUTE_ANIMATIONS_ELEMENTS;
  userForm: FormGroup;
  users$: Observable<User[]>;
  isEdit$: Observable<{ value: boolean }>;

  constructor(private fb: FormBuilder, private userService: UserService) {}

  ngOnInit() {
    this.users$ = this.userService.users$;

    this.userForm = this.fb.group({
      id: '',
      username: ['', [Validators.required, Validators.minLength(5)]],
      name: ['', [Validators.required, Validators.minLength(5)]],
      surname: ['', [Validators.required, Validators.minLength(5)]]
    });

    this.isEdit$ = this.userForm.get('id').valueChanges.pipe(
      startWith(''),
      map(id => ({ value: (id || '').length > 0 }))
    );
  }

  removeUser(id: string) {
    this.userService.removeUser(id);
  }

  editUser(user: User) {
    this.userForm.patchValue({ ...user });
  }

  onSubmit(userFormRef: FormGroupDirective) {
    if (this.userForm.valid) {
      const data = this.userForm.getRawValue();
      if (data.id && data.id.length) {
        this.userService.updateUser(data);
      } else {
        this.userService.addUser({ ...data });
      }
      userFormRef.resetForm();
      this.userForm.reset();
    }
  }

  trackByUserId(index: number, user: User): string {
    return user.id;
  }
}
