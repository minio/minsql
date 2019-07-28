import {Injectable} from '@angular/core';
import {CanActivate, Router} from '@angular/router';
import {select, Store} from '@ngrx/store';
import {Observable} from 'rxjs';

import {selectIsAuthenticated} from './auth.selectors';
import {AppState} from '../core.state';
import {ApiService} from '../api-service/api.service';
import {AppStateService} from '../app-state/app-state.service';

@Injectable({
  providedIn: 'root'
})
export class AuthGuardService implements CanActivate {
  constructor(private store: Store<AppState>, private state: AppStateService, private apiService: ApiService, private router: Router) {


    const access_key = localStorage.getItem('access_key');
    const secret_key = localStorage.getItem('secret_key');
    if (access_key == null || secret_key == null) {
      this.state.isAuthenticated.next(false);
      this.router.navigate(['/login']);
    }

    this.apiService.get(`/api/tokens/${access_key}`).subscribe(
      (resp) => {
        this.state.isAuthenticated.next(true);
        this.router.navigate(['/']);
      },
      (nono) => {
        // no op
      })

  }

  canActivate(): Observable<boolean> {
    return this.state.isAuthenticated.asObservable();
  }
}
