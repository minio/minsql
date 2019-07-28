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

import { TestBed } from '@angular/core/testing';

import { UserService } from './user.service';

describe('UserService', () => {
  let service: UserService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [UserService]
    });

    service = TestBed.get(UserService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should provide initial users', done => {
    service.users$.subscribe(users => {
      expect(users.length).toBe(3);
      done();
    });
  });

  it('should add user', done => {
    service.addUser({ username: 'test', name: 'Test', surname: 'Tester' });
    service.users$.subscribe(users => {
      expect(users.length).toBe(4);
      expect(users[3].username).toBe('test');
      done();
    });
  });
});
