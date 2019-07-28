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

import {ChangeDetectionStrategy, Component, Inject, OnInit} from '@angular/core';
import {MAT_DIALOG_DATA, MatDialogRef} from '@angular/material';


export class CodeGenData {
  query: string;
}


@Component({
  selector: 'minsql-codegen-modal',
  templateUrl: './codegen-modal.component.html',
  styleUrls: ['./codegen-modal.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class CodegenModalComponent implements OnInit {


  constructor(
    public dialogRef: MatDialogRef<CodegenModalComponent>,
    @Inject(MAT_DIALOG_DATA) public data: CodeGenData) {
  }

  onNoClick(): void {
    this.dialogRef.close();
  }


  ngOnInit() {
  }

}
