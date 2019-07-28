import {AfterViewInit, ChangeDetectionStrategy, Component, OnInit, ViewChild} from '@angular/core';
import {MatPaginator} from '@angular/material';
import {Router} from '@angular/router';
import {tap} from 'rxjs/operators';
import {DataStore, Token} from '../../../shared/structs';
import {TokensDataSource} from '../../../shared/datasources';
import {ApiService} from '../../../core/api-service/api.service';

@Component({
  selector: 'minsql-tokens',
  templateUrl: './tokens-list.component.html',
  styleUrls: ['./tokens-list.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TokensListComponent implements OnInit, AfterViewInit {
  displayedColumns: string[] = ['access_key', 'description', 'is_admin', 'enabled'];
  data: Token[];
  dataSource: TokensDataSource;

  @ViewChild(MatPaginator, {static: false}) paginator: MatPaginator;

  constructor(public apiService: ApiService, public router: Router) {
    this.dataSource = new TokensDataSource(this.apiService);
  }

  ngOnInit() {
    this.dataSource.loadElements();
  }

  ngAfterViewInit() {
    this.paginator.page
      .pipe(
        tap(() => this.loadData())
      )
      .subscribe();
  }

  loadData() {
    this.dataSource.loadElements(
      this.paginator.pageIndex,
      this.paginator.pageSize
    );
  }

  clickOn(row: DataStore) {
    this.router.navigate(['configuration', 'tokens', row.access_key]);
  }

  add() {
    this.router.navigate(['configuration', 'tokens', 'new']);
  }

}
