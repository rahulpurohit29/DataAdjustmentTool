import { Component } from '@angular/core';
import { HttpClient} from '@angular/common/http';
@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  file: File = null;
  jsondata=null;
  ids=[];
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
      this.jsondata=null;
      this.ids=[];
      let formdata=new FormData();
      formdata.append('csv_file',this.file); //Creating the body of the POST request to be submitted
      this.httpRequest.post('http://127.0.0.1:8000/update_csv',formdata).subscribe(
        response=>{
          console.log("Success");
          alert(response['message']);
        },
        err=>{
          console.log("Error");
          alert(err['message']);
        }
      ); // Sending the POST request to the URL with the file and it's Details
    }
  }

  Reset(){
    this.jsondata=null;
    this.ids=[];
  }
  onAdd(){
    if(this.file===null)
    {
      window.alert("Select a file first!!!");
      return false;
    }
    else
    {
      this.jsondata=null;
      this.ids=[];
      let formdata=new FormData();
      formdata.append('csv_file',this.file); //Creating the body of the POST request to be submitted
      this.httpRequest.post('http://127.0.0.1:8000/add_csv',formdata).subscribe(
        response=>{
          console.log("Success");
          alert(response['message']);
        },
        err=>{
          console.log("Error");
          alert(err['message']);
        }
      ); // Sending the POST request to the URL with the file and it's Details
    }
  }
  Download(){
    this.httpRequest.get('http://127.0.0.1:8000/download_csv').subscribe(
      response=>{
          console.log("Success");
          this.jsondata=response;
          console.log(this.jsondata);
          console.log(typeof(this.jsondata));
          for(let key in this.jsondata)
          {
            if(this.jsondata.hasOwnProperty(key)){
              console.log(this.jsondata[key].stud_id);
              this.ids.push(this.jsondata[key].stud_id)
            }
          }
      },
      err=>{
        console.log("Error");
        alert("Unable to get data from the Server.");
      }
    );
    this.jsondata=null;
    this.ids=[];
  }
}
