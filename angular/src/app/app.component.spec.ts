import { TestBed, async } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { AppComponent } from './app.component';
import { BrowserModule } from '@angular/platform-browser';
import { HttpClientModule } from '@angular/common/http';
import { AppRoutingModule } from './app-routing.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { not } from '@angular/compiler/src/output/output_ast';

describe('AppComponent', () => {
  let app: AppComponent;
  let fixture;
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
    }).compileComponents().then( () => {
      fixture = TestBed.createComponent(AppComponent);
      app = fixture.componentInstance;
  });
  }));

  it('should create the app', () => {
    const fixture = TestBed.createComponent(AppComponent);
    const app = fixture.componentInstance;
    expect(app).toBeTruthy();
  });
  it(`should have class 'container'`, ()=>{
    const DivElement=<HTMLDivElement>document.getElementsByClassName('container')[0];
    expect(DivElement).toBeDefined();
  }
  );
  it(`should have text 'DATA ADJUSTMENT TOOL'`, ()=>{
    const HeadingElement=<HTMLHeadingElement>document.getElementById('heading');
    expect(HeadingElement.textContent).toBe("DATA ADJUSTMENT TOOL");
  }
  );
  it(`should have label 'CSV update'`, ()=>{
    const LabelElement=<HTMLLabelElement>document.getElementById("label");
    expect(LabelElement.textContent).toBe("CSV update");
  }
  );
  it(`should have called onClick() function on click of update`, ()=>{
    let spyupdate=spyOn(app,'onClick').and.callThrough();
    expect(spyupdate).not.toHaveBeenCalled();
    document.getElementById('update').click();
    expect(spyupdate).toHaveBeenCalled();
  });
  it(`should have called onAdd() function on click of add`, ()=>{
    let spyadd=spyOn(app,'onAdd').and.callThrough();
    expect(spyadd).not.toHaveBeenCalled();
    document.getElementById('add').click();
    expect(spyadd).toHaveBeenCalled();
  });
  it(`should have called Download() function on click of Download`, ()=>{
    let spydownload=spyOn(app,'Download').and.callThrough();
    expect(spydownload).not.toHaveBeenCalled();
    document.getElementById('download').click();
    expect(spydownload).toHaveBeenCalled();
  });
  it(`should have reset by click on reset`, ()=>{
    document.getElementById('Reset').click();
    let changeinput=<HTMLInputElement>document.getElementById('change-input');
    expect(changeinput.value).toEqual('');
  });
  it(`should have CSS`,()=>{
      const styling=getComputedStyle(document.getElementById('change-input'));
      expect(styling).not.toEqual('');
  }
  );
});