import {Injectable} from '@angular/core';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {Observable} from 'rxjs';
import {Patient} from '../models/patient';

@Injectable({
  providedIn: 'root'
})

export class PatientsService {
  baseUrl = 'http://127.0.0.1:3001';
  readonly headers = new HttpHeaders()
    .set('Content-Type', 'application/json');
  patientData: Patient[] = [];

  constructor(private http: HttpClient) {}

  getAll(): Observable<Patient[]> {

    return this.http.get<Patient[]>(this.baseUrl.concat('/Patient'));
  }

  getPatientByID(id: string): Observable<Patient> {
    console.log(id);
    return this.http.get<Patient>(`${this.baseUrl}/Patient/${id}`);
  }
}

