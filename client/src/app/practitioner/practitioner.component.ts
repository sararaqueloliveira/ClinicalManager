import { Component } from '@angular/core';
import { MatDialog } from "@angular/material/dialog";
import { Subscription } from "rxjs";
import {Practitioner} from '../models/practitioner';
import {PractitionerService} from './practitioner.service';
import {Patient} from '../models/patient';
import {ActivatedRoute, Router} from '@angular/router';
//import {practitionerDialogComponent} from '../practitionerDialog/practitionerDialog.component';

@Component({
	selector: 'app-root',
	templateUrl: './practitioner.component.html',
	styleUrls: ['./practitioner.component.scss'],

})

export class PractitionerComponent {

  practitionerData: Practitioner[] = [];

  columnsToDisplay = ['gender_flag', 'name', 'id', 'birthDate', 'encounters'];
  getAllSubscription: Subscription;
  flag: boolean;

  config: any;

  constructor(public dialog: MatDialog, public service: PractitionerService, public practitioner: Practitioner, private router:Router) {
    this.config = {
      itemsPerPage: 10,
      currentPage: 1,
      totalItems: this.practitionerData.length,
    };
  }

  ngOnInit(): void {
    this.loadPractitionerList();
    console.log(this.practitionerData);
  }

  private loadPractitionerList(): void {
    this.getAllSubscription = this.service.getAll()
      .subscribe(practitioners  => {
        for(let key in practitioners['entry']) {
          let practitionerEntries = practitioners['entry'][key]['resource'];
          if(practitionerEntries['gender'] == "male")
            this.flag = false;
          else {
            this.flag = true;
          }
          this.practitionerData.push(new Practitioner(
            practitionerEntries['id'],
            practitionerEntries['name'][0]['given'],
            practitionerEntries['gender'],
            practitionerEntries['address'][0]['city'],
            practitionerEntries['address'][0]['state'],
            practitionerEntries['address'][0]['postalCode'],
            this.flag,
            practitionerEntries['active']
            )
          );
        }
    });
  }

  pageChanged(event){
    this.config.currentPage = event;
  }

  routeToEncountersComponent(id_practitioner) {
    this.router.navigate(['/encounters'],
      {queryParams: {practitioner: id_practitioner}
      });
  }
}


