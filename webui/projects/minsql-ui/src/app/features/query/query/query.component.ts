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

import {ChangeDetectionStrategy, Component, OnInit, ViewChild} from '@angular/core';


import {BehaviorSubject, Observable, of} from 'rxjs';
import {FormControl} from '@angular/forms';
import {catchError, finalize, map, startWith} from 'rxjs/operators';
import {MatAutocompleteSelectedEvent, MatAutocompleteTrigger, MatDialog} from '@angular/material';
import {ApiService} from '../../../core/api-service/api.service';
import {ROUTE_ANIMATIONS_ELEMENTS} from '../../../core/core.module';
import {CodegenComponent} from '../codegen/codegen.component';


export class QuerySpan {
  spanType: string;
  value?: QSpanValue;

  subsequentSpans: string[];
  options: QSpanValue[];

  public constructor(spanType: string, subsequentSpans: string[], options: QSpanValue[], value?: QSpanValue) {
    this.spanType = spanType;
    this.subsequentSpans = subsequentSpans;
    this.options = options;
    this.value = value;
  };

  static copy(src: QuerySpan): QuerySpan {
    const c: QuerySpan = new QuerySpan(
      src.spanType,
      src.subsequentSpans,
      src.options,
      src.value
    );
    return c;
  }

  getOptions(stack: QuerySpan[]): QSpanValue[] {
    return this.options;
  }

}

export class QSpanValue {
  name: string;
  valueType: string;
  description?: string;

  constructor(name: string, valueType: string, description?: string) {
    this.name = name;
    this.valueType = valueType;
    this.description = description;
  };

  static copy(src: QSpanValue): QSpanValue {
    const c: QSpanValue = {
      name: src.name,
      valueType: src.valueType,
      description: src.description,
    };
    return c;
  }
}

export class ConditionFieldSpan extends QuerySpan {

  getOptions(stack: QuerySpan[]): QSpanValue[] {
    const opts: QSpanValue[] = [];
    for (const opt of stack) {
      if (opt.spanType === 'smartField') {
        opts.push(opt.value);
      }
      if (opt.spanType === 'from') {
        break;
      }
    }
    return opts;
  }
}

@Component({
  selector: 'minsql-query',
  templateUrl: './query.component.html',
  styleUrls: ['./query.component.scss'],
  changeDetection: ChangeDetectionStrategy.Default
})
export class QueryComponent implements OnInit {
  routeAnimationsElements = ROUTE_ANIMATIONS_ELEMENTS;

  outputQuery = '';
  flipper = false;

  stateCtrl = new FormControl();
  filteredStates: Observable<QSpanValue[]>;

  displayedColumns: string[] = [];
  resultingData: object[] = null;

  loadingSubject = new BehaviorSubject<boolean>(false);

  currentRequest = null;

  @ViewChild(MatAutocompleteTrigger, {static: false}) qbfield: MatAutocompleteTrigger;


  spanDirectory: { [key: string]: QuerySpan } = {
    select: new QuerySpan(
      'select',
      ['smartField'],
      [
        new QSpanValue(
          'SELECT',
          'select'
        )
      ]
    ),

    smartField: new QuerySpan(
      'smartField',
      ['smartField', 'from'],
      [
        {
          name: '$ip',
          valueType: '$ip',
          description: 'IP address',
        },
        {
          name: '$email',
          valueType: '$email',
          description: 'Email address',
        },
        {
          name: '$date',
          valueType: '$date',
          description: 'Date',
        },
        {
          name: '$url',
          valueType: '$url',
          description: 'Web URIs',
        },
        {
          name: '$quoted',
          valueType: '$quoted',
          description: 'Any quoted text',
        },
        {
          name: '$phone',
          valueType: '$phone',
          description: 'A valid US Phone number',
        },
        {
          name: '$user_agent',
          valueType: '$user_agent',
          description: 'Browser\'s User Agent',
        },
        {
          name: '$user_agent.name',
          valueType: '$user_agent.name',
          description: 'Browser\'s Name',
        },
        {
          name: '$user_agent.category',
          valueType: '$user_agent.category',
          description: 'Browser\'s Category (pc, mac)',
        },
        {
          name: '$user_agent.version',
          valueType: '$user_agent.version',
          description: 'Browser\'s Version',
        },
        {
          name: '$user_agent.vendor',
          valueType: '$user_agent.vendor',
          description: 'Browser\'s Vendor',
        },
        {
          name: '$user_agent.os',
          valueType: '$user_agent.os',
          description: 'Browser\'s Operative System',
        },
        {
          name: '$user_agent.os_version',
          valueType: '$user_agent.os_version',
          description: 'Browser\'s Operative System Version',
        },
        {
          name: '*',
          valueType: '*',
          description: 'All fields',
        },
      ]
    ),
    from: new QuerySpan(
      'from',
      ['logs'],
      [
        {
          name: 'FROM',
          valueType: 'FROM',
        }
      ],
    ),
    logs: new QuerySpan(
      'logs',
      ['where'],
      [{
        name: 'fakelog1',
        valueType: 'log',
      }],
    ),
    where: new QuerySpan(
      'where',
      ['conditionField'],
      [
        {
          name: 'WHERE',
          valueType: 'WHERE',
        }
      ],
    ),
    // we are going to insert the conditionFieldSpan from the constructor since it needs to reference the span stack
    conditionOperator: new QuerySpan(
      'conditionOperator',
      ['conditionValue'],
      [
        {
          name: '=',
          description: 'Equal to...',
          valueType: 'operator',
        },
        {
          name: '!=',
          description: 'Is not equal to...',
          valueType: 'operator',
        },
        {
          name: 'LIKE',
          description: 'is like ',
          valueType: 'operator',
        },
      ],
    ),
    conditionValue: new QuerySpan(
      'conditionValue',
      ['limit'],
      [],
    ),
    and: new QuerySpan(
      'and',
      ['conditionField'],
      [
        {
          name: 'AND',
          valueType: 'AND',
        }
      ],
    ),
    or: new QuerySpan(
      'or',
      ['conditionField'],
      [
        {
          name: 'OR',
          valueType: 'OR',
        }
      ],
    ),
    limit: new QuerySpan(
      'limit',
      [],
      [
        {
          name: 'LIMIT',
          valueType: 'LIMIT',
        }
      ],
    ),
    limitValue: new QuerySpan(
      'limitValue',
      // no subsequents
      [],
      [],
    ),
  };

  currentSpan: QuerySpan = null;

  spanStack: QuerySpan[] = [];

  constructor(private apiService: ApiService, private dialog: MatDialog) {
    // get a list of logs
    this.apiService.get('/api/logs?limit=1000').subscribe((response) => {
      if (response.hasOwnProperty('results') === false) {
        return
      }
      this.spanDirectory['logs'].options = [];
      for (let log of response['results']) {
        this.spanDirectory['logs'].options.push({
          name: log.name,
          valueType: 'log',
        });
      }
    });

    const conditionFieldSpan = new ConditionFieldSpan(
      'conditionField',
      ['conditionOperator'],
      []
    );
    this.spanDirectory['conditionField'] = conditionFieldSpan;

    this.filteredStates = this.stateCtrl.valueChanges
      .pipe(
        startWith(''),
        map(state => {
          if (state === '') {
            this.resetBuilder();
            return this.filterableOptions.slice();
          }
          return state ? this._filterStates(state) : this.filterableOptions.slice();
        })
      );
  }

  filterableOptions: QSpanValue[] = [];

  initFilterValues(span: QuerySpan) {
    this.filterableOptions = [];
    for (const spanId of this.currentSpan.subsequentSpans) {
      if (this.spanDirectory.hasOwnProperty(spanId) === false) {
        continue;
      }
      const nextSpan = this.spanDirectory[spanId];
      for (const opt of nextSpan.getOptions(this.spanStack)) {
        this.filterableOptions.push(opt);
      }
    }
  }

  ngOnInit() {
    this.resetBuilder();
  }

  resetBuilder() {
    this.spanStack = [];
    const selectSpan = QuerySpan.copy(this.spanDirectory['select']);
    selectSpan.value = selectSpan.getOptions(this.spanStack)[0];

    this.spanSelected(selectSpan);
  }

  spanSelected(span: QuerySpan) {
    this.spanStack.push(span);
    this.currentSpan = span;
    this.outputQuery = this.flattenStack();
    this.initFilterValues(this.currentSpan);
    this.stateCtrl.setValue(this.outputQuery);

    // this.qbfield.openPanel();
  }

  flattenStack() {
    let output = '';
    let lastSpanType: string = null;
    for (const sp of this.spanStack) {
      // output += ' '
      if (sp.value == null && sp.getOptions(this.spanStack).length === 1) {
        sp.value = sp.getOptions(this.spanStack)[0];
      }
      if (sp.spanType === 'smartField' && lastSpanType === 'smartField') {
        output = output.substr(0, output.length - 1);
        output += ', ' + sp.value.name + ' ';
      } else {
        output += sp.value.name + ' ';
      }

      lastSpanType = sp.spanType;
    }
    output = output.trimLeft();
    return output;
  }

  optionSelected(val: MatAutocompleteSelectedEvent) {

    // make a copy of the selected span
    let spanValue: QSpanValue = val.option.value;

    // look up who owns this value from possible
    let owningQSpan: QuerySpan = null;
    for (const spanId of this.currentSpan.subsequentSpans) {
      const nextSpan = this.spanDirectory[spanId];
      for (const opt of nextSpan.getOptions(this.spanStack)) {
        if (opt === spanValue) {
          owningQSpan = QuerySpan.copy(nextSpan);
          break;
        }
      }
    }
    // make a copy of the value to make it ours and save the value to the new span.
    spanValue = QSpanValue.copy(spanValue);

    this.addSpanToStack(owningQSpan, spanValue);

    return false;
  }

  addSpanToStack(querySpan: QuerySpan, spanValue: QSpanValue) {
    querySpan.value = spanValue;
    // if we are still in the projection side of the query (SELECT projection FROM) check for position increase for duplicate
    // value types
    let beyondFrom = false;
    for (const qspan of this.spanStack) {
      if (qspan.spanType === 'from') {
        beyondFrom = true;
      }
    }

    if (beyondFrom === false) {
      // check there's another value like this one inside the stack
      let currentPosition = 1;
      for (const qspan of this.spanStack) {
        if (qspan.spanType === 'smartField' && qspan.value.valueType === spanValue.valueType) {
          currentPosition++;
        }
        // don't scan beyond FROM
        if (qspan.spanType === 'from') {
          break;
        }
      }
      if (currentPosition > 1) {
        spanValue.name = spanValue.name + currentPosition;
      }
    }

    if (querySpan != null) {
      this.spanSelected(querySpan);
    }
  }

  private _filterStates(value: string): QSpanValue[] {
    if (typeof value === 'string') {
      if (value.trim() === '') {
        this.resetBuilder();
      } else if (value.indexOf(this.outputQuery) >= 0) {
        let deltaValue = value.substr(this.outputQuery.length, value.length).trimLeft();
        if (deltaValue.startsWith(',')) {
          deltaValue = deltaValue.substr(1);
        }
        deltaValue = deltaValue.toLocaleLowerCase();
        console.log(`delta string \`${deltaValue}\``);

        const quotedValueRegex = /\'(.*?)\'/gm;
        const integerValueRegex = /(\d+) /gm;
        if (['conditionOperator', 'limit'].includes(this.currentSpan.spanType) &&
          (deltaValue.match(quotedValueRegex) || deltaValue.match(integerValueRegex))) {
          // New Value Detected
          const spanValue = new QSpanValue(deltaValue.trim(), this.currentSpan.spanType);
          let owningQspan: QuerySpan = null;
          if (this.currentSpan.spanType === 'limit') {
            owningQspan = this.spanDirectory['limitValue'];
          } else {
            owningQspan = this.spanDirectory['conditionValue'];
          }
          // turn on AND and OR
          const andOrIncluded = owningQspan.subsequentSpans.filter(Set.prototype.has, new Set(['and', 'or']));
          if (andOrIncluded.length === 0) {
            owningQspan.subsequentSpans.push('and');
            owningQspan.subsequentSpans.push('or');
          }
          owningQspan = QuerySpan.copy(owningQspan);
          this.addSpanToStack(owningQspan, spanValue);
        }

        return this.filterableOptions.filter(state => state.name.toLowerCase().indexOf(deltaValue) >= 0);
      } else {
        return this.filterableOptions.filter(state => true);
      }
    } else {
      return this.filterableOptions.filter(state => true);
    }

  }

  cancelRequest() {
    if (this.currentRequest !== null) {
      this.currentRequest.unsubscribe();
    }
  }

  submitQuery() {
    // this.outputQuery = 'SELECT $ip FROM mylog ';
    this.cancelRequest();
    this.loadingSubject.next(true);
    this.currentRequest = this.apiService.post('/search', this.outputQuery, {
      headers: {'MINSQL-PREVIEW': 'true'},
      responseType: 'text'
    }).pipe(
      catchError(() => of([])),
      finalize(() => this.loadingSubject.next(false))
    ).subscribe(
      (resp: string) => {
        this.resultingData = [];
        try {
          resp.split('\n').forEach(value => {
            if (value.trim() !== '') {
              try {
                this.resultingData.push(JSON.parse(value));
              } catch (e) {
                console.log('error', value, e);
              }
            }
          });
          if (this.resultingData.length > 0) {
            this.displayedColumns = Object.keys(this.resultingData[0]);
          }
        } catch (e) {
          console.log('error', e);
          this.resultingData = null;
          this.displayedColumns = [];
        }

      },
      (err) => {
        console.log('error', err);
        this.resultingData = null;
        this.displayedColumns = [];
      }
    );

  }

  gencode($event) {
    if ($event !== null) {
      $event.stopPropagation();
    }
    const diaDataStoreRef = this.dialog.open(CodegenComponent, {
      width: '800px',
      data: {
        query: this.outputQuery.trim()
      }
    });
    return false;
  }


}
