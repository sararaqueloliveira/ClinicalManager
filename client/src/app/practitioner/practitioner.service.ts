import {Injectable} from '@angular/core';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {Observable} from 'rxjs';
import {Practitioner} from '../models/practitioner';

@Injectable({
  providedIn: 'root'
})

export class PractitionerService {
  baseUrl = 'http://127.0.0.1:3001';
  readonly headers = new HttpHeaders()
    .set('Content-Type', 'application/json');
  patientData: Practitioner[] = [];

  constructor(private http: HttpClient) {}

  getAll(): Observable<Practitioner[]> {
    return this.http.get<Practitioner[]>(this.baseUrl.concat('/Practitioner'));
  }

  get(id: string): Observable<Practitioner> {
    return this.http.get<Practitioner>(`${this.baseUrl}/Practitioner/${id}`);
  }

  /*
  search(nome: string, freguesia: string): Observable<Patient> {
                                            // Ex.: http://127.0.0.1:5000/Patientssearch?PatientNome=I&PatientFreguesia=L
    return this.http.get<Patient>(`${this.baseUrl}/Patientssearch?PatientNome=${nome}&PatientFreguesia=${freguesia}`);
  }

  getMostSearched(): Observable<Patient[]> {
    return this.http.get<Patient[]>(`${this.baseUrl}/Patientsmostsearched`);
  }
*/
}
