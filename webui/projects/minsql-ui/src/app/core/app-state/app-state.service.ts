import { Injectable } from '@angular/core';
import {BehaviorSubject} from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class AppStateService {

  isAuthenticated = new BehaviorSubject<boolean>(false);

  constructor() { }
}
