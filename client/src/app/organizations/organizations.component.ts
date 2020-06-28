import { Component } from '@angular/core';
import { Subscription } from "rxjs";
import {Organization} from '../models/organization';
import {OrganizationsService} from './organizations.service';

@Component({
	selector: 'app-root',
	templateUrl: './organizations.component.html',
	styleUrls: ['./organizations.component.scss'],
})

export class OrganizationsComponent {

  organizationData: Organization[] = [];

  columnsToDisplay = ['name', 'id', 'localization', 'phone'];
  getAllSubscription: Subscription;

  config: any;

  constructor( public service: OrganizationsService, public organization: Organization) {
    this.config = {
      itemsPerPage: 10,
      currentPage: 1,
      totalItems: this.organizationData.length,
    };
  }

  ngOnInit(): void {
    this.loadOrganizationsList();
    console.log(this.organizationData);
  }

  private loadOrganizationsList(): void {
    this.getAllSubscription = this.service.getAll()
      .subscribe(organizations  => {
        for(let key in organizations['entry']) {
          let organizationsEntries = organizations['entry'][key]['resource'];
          this.organizationData.push(new Organization(
            organizationsEntries['id'],
            organizationsEntries['name'],
            organizationsEntries['address'][0]['city'],
            organizationsEntries['address'][0]['state'],
            organizationsEntries['address'][0]['postalCode'],
            organizationsEntries['telecom'][0]['value']
            )
          );
        }
    });
  }

  pageChanged(event){
    this.config.currentPage = event;
  }
}


