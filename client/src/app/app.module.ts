import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';

import { AppComponent } from './app.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { MatTableModule } from '@angular/material/table';
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';
import { MatFormFieldModule } from '@angular/material/form-field';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { HttpClientModule } from '@angular/common/http';
import { MatToolbarModule } from '@angular/material/toolbar';
import { RouterModule, Routes } from '@angular/router';
import { PageNotFoundComponent } from './page-not-found/page-not-found.component';
import { MatListModule } from '@angular/material/list';
import { MatCardModule } from '@angular/material/card';
import { MatPaginator, MatPaginatorModule } from '@angular/material/paginator';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { AgmCoreModule } from '@agm/core';
import {MatExpansionModule} from '@angular/material/expansion';
import { PatientsComponent } from './patients/patients.component';
import {Patient} from './models/patient';
import {Practitioner} from './models/practitioner';
import { NgxPaginationModule } from 'ngx-pagination';
import {PatientDialogComponent} from './patientDialog/patientDialog.component';
import {MatGridListModule, MatSelectModule, MatSidenavModule} from '@angular/material';
import {PractitionerComponent} from './practitioner/practitioner.component';
import {OrganizationsComponent} from './organizations/organizations.component';
import {Organization} from './models/organization';
import {Encounter} from './models/encounter';
import {EncounterComponent} from './encounter/encounter.component';
import {PatientDetail} from './models/patientDetails';
import {PractitionerDetails} from './models/practitionerDetails';


const appRoutes: Routes = [
  { path: 'patients', component: PatientsComponent },
  { path: 'practitioners', component: PractitionerComponent },
  { path: 'organizations', component: OrganizationsComponent },
  { path: 'encounters', component: EncounterComponent },

  {
    path: '',
    redirectTo: '/patients',
    pathMatch: 'full'
  },
  { path: '**', component: PageNotFoundComponent }
];

@NgModule({
  declarations: [
    AppComponent,
    PatientsComponent,
    PractitionerComponent,
    PageNotFoundComponent,
    PatientDialogComponent,
    OrganizationsComponent,
    EncounterComponent,
  ],
  imports: [
    RouterModule.forRoot(
      appRoutes,
      {enableTracing: true} // <-- debugging purposes only
    ),
    NgxPaginationModule,
    MatInputModule,
    MatDialogModule,
    MatTableModule,
    BrowserModule,
    BrowserAnimationsModule,
    MatButtonModule,
    MatFormFieldModule,
    FormsModule,
    HttpClientModule,
    MatToolbarModule,
    ReactiveFormsModule,
    MatListModule,
    MatCardModule,
    MatPaginatorModule,
    NgbModule,
    BrowserModule,
    AgmCoreModule.forRoot({
      apiKey: ''
      // tslint:disable-next-line:indent
    }),
    // tslint:disable-next-line:indent
    MatExpansionModule,
    MatGridListModule,
    MatSidenavModule,
    MatSelectModule
  ],
  providers: [
    Patient, Practitioner, Organization, Encounter, PatientDetail, PractitionerDetails
  ],
  bootstrap: [AppComponent],
  entryComponents: [PatientDialogComponent]
})
export class AppModule {
}
