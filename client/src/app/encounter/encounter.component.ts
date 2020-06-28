import { Component } from '@angular/core';
import { MatDialog } from "@angular/material/dialog";
import { Subscription } from "rxjs";
import {Patient} from '../models/patient';
import {PatientDetail} from '../models/patientDetails';
import {EncounterService} from './encounter.service';
import {PatientsService} from '../patients/patients.service';
import {Encounter} from '../models/encounter';
import {ActivatedRoute, Router} from '@angular/router';
import {FormControl} from '@angular/forms';
import {PractitionerService} from '../practitioner/practitioner.service';
import {PractitionerDetails} from '../models/practitionerDetails';

@Component({
	selector: 'app-root',
	templateUrl: './encounter.component.html',
	styleUrls: ['./encounter.component.scss'],

})

export class EncounterComponent {

  encounterData: Encounter[] = [];
  patientData: Patient[] = [];

  patientsName: PatientDetail[] = [];
  practitionersName: PractitionerDetails[] = [];

  active_flag = false;
  flag_patient = false;

  columnsToDisplay = ['patient', 'practitioner', 'type', 'hour'];
  getAllSubscription: Subscription;
  getPatientSubscription: Subscription;
  getPractitionerSubscription: Subscription;

  config: any;

  private patient_id;
  private practitioner_id;
  flag: boolean;

  patients = new FormControl();
  selected_patient: any;

  constructor(public service: EncounterService, public encounter: Encounter, private activatedRoute: ActivatedRoute, private route:ActivatedRoute,private router:Router, public pat_service: PatientsService, public pract_service: PractitionerService) {
    this.config = {
      itemsPerPage: 10,
      currentPage: 1,
      totalItems: this.encounterData.length,
    };
  }

  ngOnInit(): void {
    this.activatedRoute.queryParams.subscribe(params => {
      this.patient_id = params['patient'];
      this.practitioner_id = params['practitioner'];
    });

    this.getPatientsName();
    this.getPractitionerName();

    if(this.patient_id != undefined && this.practitioner_id == undefined) {
      this.loadEncountersByPatient(this.patient_id);
      this.flag_patient = true;
      this.active_flag = true;

    } else if(this.patient_id == undefined && this.practitioner_id != undefined) {
      this.loadEncountersByPractitioner(this.practitioner_id);
      this.active_flag = true;
      this.flag_patient = false;

    } else {
      console.log("nenhum dos dois");
    }
  }

  private loadEncountersByPatient(patient_id) {
    this.getAllSubscription = this.service.getByPatient(patient_id)
      .subscribe(encounters => {
        for (let key in encounters['entry']) {
          let encounterEntries = encounters['entry'][key]['resource'];

            let str = encounterEntries['participant'][0]['individual']['reference'];
            let splitted = str.split("/", 2);

            if (encounterEntries['status'] == "finished")
              this.flag = true;
            else {
              this.flag = false;
            }
            this.encounterData.push(new Encounter(
              encounterEntries['id'],
              this.flag,
              encounterEntries['type'][0]['text'],
              this.patient_id,
              splitted[1],
              encounterEntries['period']['start'],
              encounterEntries['period']['end']
            ));
          }
      });
  }

  private loadEncountersByPractitioner(practitioner_id) {
    this.getPractitionerSubscription = this.service.getByPractitioner(practitioner_id)
      .subscribe(encounters => {
        for (let key in encounters['entry']) {
          let encounterEntries = encounters['entry'][key]['resource'];
          let str = encounterEntries['subject']['reference'];
          let splitted = str.split("/", 2);
          if (encounterEntries['status'] == "finished")
            this.flag = true;
          else {
            this.flag = false;
          }
          this.encounterData.push(new Encounter(
            encounterEntries['id'],
            this.flag,
            encounterEntries['type'][0]['text'],
            splitted[1],
            this.practitioner_id,
            encounterEntries['period']['start'],
            encounterEntries['period']['end']
          ));
        }
      });
  }

  public getPatientName(id) {
    this.getPatientSubscription = this.pat_service.getPatientByID(id)
      .subscribe(patients  => {
        this.patientData.push(new Patient(
          patients['id'],
          patients['name'][0]['given'][0],
          patients['gender'],
          patients['birthDate'],
          patients['address'][0]['city'],
          patients['address'][0]['state'],
          patients['address'][0]['postalCode'],
          false,
          patients['identifier'][2]['value'],
          patients['telecom'][0]['value'],
          patients['maritalStatus']['text'],
          patients['communication'][0]['language']['text']
          )
        );
    });
  }

  private loadEncountersList(): void {
    /*
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
    });*/
  }

  pageChanged(event){
    this.config.currentPage = event;
  }

  private getPatientsName() {
    this.getAllSubscription = this.pat_service.getAll()
      .subscribe(patients  => {
        for(let key in patients['entry']) {
          let patientEntries = patients['entry'][key]['resource'];
          this.patientsName.push(new PatientDetail(
            patientEntries['id'],
            patientEntries['name'][0]['given'][0]
            )
          )
        }
      });
  }

  private getPractitionerName() {
    this.getAllSubscription = this.pract_service.getAll()
      .subscribe(patients  => {
        for(let key in patients['entry']) {
          let patientEntries = patients['entry'][key]['resource'];
          this.practitionersName.push(new PractitionerDetails(
            patientEntries['id'],
            patientEntries['name'][0]['given'][0]
            )
          )
        }
      });
    console.log(this.practitionersName);
  }

  routeToEncountersComponent(id_patient) {
    this.router.navigate(['/patients'],
      {queryParams: {patient_encounter: id_patient}
      });
  }

}


