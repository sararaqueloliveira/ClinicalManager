import { Component } from '@angular/core';
import { MatDialog } from "@angular/material/dialog";
import { Subscription } from "rxjs";
import {Patient} from '../models/patient';
import {PatientsService} from './patients.service';
import {PatientDialogComponent} from '../patientDialog/patientDialog.component';
import {Encounter} from '../models/encounter';
import {EncounterComponent} from '../encounter/encounter.component';
import {ActivatedRoute, Router} from '@angular/router';

@Component({
	selector: 'app-root',
	templateUrl: './patients.component.html',
	styleUrls: ['./patients.component.scss'],

})

export class PatientsComponent {

  patientData: Patient[] = [];
  patient_id;

  columnsToDisplay = ['gender_flag', 'name', 'id', 'birthDate', 'view', 'encounters'];
  getAllSubscription: Subscription;
  flag: boolean;

  config: any;

  constructor(public dialog: MatDialog, public service: PatientsService, public patient: Patient, private route:ActivatedRoute,private router:Router, private activatedRoute: ActivatedRoute) {
    this.config = {
      itemsPerPage: 10,
      currentPage: 1,
      totalItems: this.patientData.length
    };
  }

  ngOnInit(): void {
    this.activatedRoute.queryParams.subscribe(params => {
      this.patient_id = params['patient_encounter'];
    });

    if(this.patient_id != undefined) {
      this.routeToEncountersComponent(this.patient_id);
    }

    this.loadPatientsList();
    console.log(this.patientData);
  }

  private loadPatientsList(): void {
    this.getAllSubscription = this.service.getAll()
      .subscribe(patients  => {
        for(let key in patients['entry']) {
          let patientEntries = patients['entry'][key]['resource'];
          if(patientEntries['gender'] == "male")
            this.flag = false;
          else {
            this.flag = true;
          }
          this.patientData.push(new Patient(
            patientEntries['id'],
            patientEntries['name'][0]['given'],
            patientEntries['gender'],
            patientEntries['birthDate'],
            patientEntries['address'][0]['city'],
            patientEntries['address'][0]['state'],
            patientEntries['address'][0]['postalCode'],
            this.flag,
            patientEntries['identifier'][2]['value'],
            patientEntries['telecom'][0]['value'],
            patientEntries['maritalStatus']['text'],
            patientEntries['communication'][0]['language']['text']
            )
          );
        }
    });
  }

  pageChanged(event){
    this.config.currentPage = event;
  }

  openDialog(patient) {
    const dialogRef = this.dialog.open(PatientDialogComponent, {
      data: {
        dataKey:
        patient
      }});

    dialogRef.afterClosed().subscribe(result => {
      console.log(`Dialog result: ${result}`);
    });
  }

  routeToEncountersComponent(id_patient) {
    this.router.navigate(['/encounters'],
      {queryParams: {patient: id_patient}
      });
  }
}
