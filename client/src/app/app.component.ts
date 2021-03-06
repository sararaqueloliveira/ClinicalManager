import { Component } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { slideInAnimation } from './animations';

@Component({
  selector: 'app-admin',
  templateUrl: 'app.component.html',
  styleUrls: ['app.component.scss'],
  animations: [ slideInAnimation ]
})
export class AppComponent {
  getAnimationData(outlet: RouterOutlet) {
    return outlet && outlet.activatedRouteData && outlet.activatedRouteData.animation;
  }
}
