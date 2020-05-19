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
      console.log("Please, Select a .csv file!!");
    }
  }
  onClick(){
    if(this.file===null)
    {
      console.log("Select a file first!!!");
    }
    else
    {
      let formdata=new FormData();
      formdata.append('File',this.file,this.file.name); //Creating the body of the POST request to be submitted
      this.httpRequest.post('google.com',formdata); // Sending the POST request to the URL with the file and it's Details
    }
  }
}