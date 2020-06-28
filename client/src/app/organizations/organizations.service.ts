import {Injectable} from '@angular/core';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {Observable} from 'rxjs';
import {Organization} from '../models/organization';

@Injectable({
  providedIn: 'root'
})

export class OrganizationsService {
  baseUrl = 'http://127.0.0.1:3001';
  readonly headers = new HttpHeaders()
    .set('Content-Type', 'application/json');

  constructor(private http: HttpClient) {}

  getAll(): Observable<Organization[]> {
    return this.http.get<Organization[]>(this.baseUrl.concat('/Organization'));
  }

  get(id: string): Observable<Organization> {
    return this.http.get<Organization>(`${this.baseUrl}/Organization/${id}`);
  }
}
