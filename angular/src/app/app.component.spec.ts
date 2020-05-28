import { TestBed, async } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { AppComponent } from './app.component';
import { BrowserModule } from '@angular/platform-browser';
import { HttpClientModule } from '@angular/common/http';
import { AppRoutingModule } from './app-routing.module';

import { HttpClientTestingModule } from '@angular/common/http/testing';
describe('AppComponent', () => {
  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        BrowserModule,
        AppRoutingModule,
        HttpClientModule
      ],
      declarations: [
        AppComponent
      ],
    }).compileComponents();
  }));

  it('should create the app', () => {
    const fixture = TestBed.createComponent(AppComponent);
    const app = fixture.componentInstance;
    expect(app).toBeTruthy();
  });
  it(`should have text 'DATA ADJUSTMENT TOOL'`, ()=>{
    const HeadingElement=<HTMLHeadingElement>document.getElementById('heading');
    expect(HeadingElement.textContent).toBe("DATA ADJUSTMENT TOOL");
  }
  );
  it(`should have label 'CSV update'`, ()=>{
    const LabelElement=<HTMLLabelElement>document.getElementById("label");
    expect(LabelElement.innerText).toBe("CSV update");
  }
  );
});
