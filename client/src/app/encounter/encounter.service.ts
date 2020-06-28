import {Injectable} from '@angular/core';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {Observable} from 'rxjs';
import {Encounter} from '../models/encounter';
import {Patient} from '../models/patient';

@Injectable({
  providedIn: 'root'
})

export class EncounterService {
  baseUrl = 'http://127.0.0.1:3001';
  readonly headers = new HttpHeaders()
    .set('Content-Type', 'application/json');

  constructor(private http: HttpClient) {}

  getAll(): Observable<Encounter[]> {
    return this.http.get<Encounter[]>(this.baseUrl.concat('/Encounter'));
  }

  getByPatient(id: string): Observable<Encounter> {
    return this.http.get<Encounter>(`${this.baseUrl}/Encounter?subject:Patient=${id}`);
  }

  getByPractitioner(id: string): Observable<Encounter> {
    return this.http.get<Encounter>(`${this.baseUrl}/Encounter?participant:Practitioner=${id}`);
  }
}
