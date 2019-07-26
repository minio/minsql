import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TokensListComponent } from './tokens.component';

describe('TokensComponent', () => {
  let component: TokensListComponent;
  let fixture: ComponentFixture<TokensListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TokensListComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TokensListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
