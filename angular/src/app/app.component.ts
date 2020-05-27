import { Component } from '@angular/core';
import { HttpClient} from '@angular/common/http';
@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  file: File= null;
  constructor(private httpRequest: HttpClient){}  //Constructor to be used to send the HTTTP Request
  FileSelect(event){
    if(event.target.files[0].name.substr( event.target.files[0].name.lastIndexOf('.') + 1)==="csv"){
      this.file=<File>event.target.files[0]; //Selecting the file thath is to be uploaded.
    }
    else{
      window.alert("Please, Select a .csv file!!");
      let changeinput=<HTMLInputElement>document.getElementById('change-input');
      changeinput.value=null;
      return false;
    }
  }
  onClick(){
    if(this.file===null)
    {
      window.alert("Select a file first!!!");
      return false;
    }
    else
    {
      let formdata=new FormData();
      formdata.append('csv_file',this.file); //Creating the body of the POST request to be submitted
      this.httpRequest.post('http://127.0.0.1:8000/upload_csv',formdata).subscribe(
        response=>{
          console.log(response);
        },
        err=>{
          console.log(err);
        }
      ); // Sending the POST request to the URL with the file and it's Details
    }
  }
  Download(){
    this.httpRequest.get('http://127.0.0.1:8000/download_csv',null).subscribe(
    );
  }
}
