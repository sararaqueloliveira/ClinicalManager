import { Component, OnDestroy} from '@angular/core';
import {MAT_DIALOG_DATA, MatDialogRef} from '@angular/material';
import { Inject } from '@angular/core';
import {Patient} from '../models/patient';

export interface DialogData {
  patient: Patient;
}

@Component({
	selector: 'patient-dialog',
	templateUrl: './patientDialog.component.html',
	styleUrls: ['./patientDialog.component.scss'],
})

export class PatientDialogComponent {

  public patient: Patient;

  constructor(public dialogRef: MatDialogRef<PatientDialogComponent>, @Inject(MAT_DIALOG_DATA) public data: DialogData) {
    this.patient = data['dataKey'];
  }

  onNoClick(): void {
    this.dialogRef.close();
  }
}


