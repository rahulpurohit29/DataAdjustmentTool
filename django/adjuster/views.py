from django.views.generic import View
from django.shortcuts import render
from django.http import HttpResponse,JsonResponse
from django.contrib import messages
import logging
import os
import glob
import findspark
findspark.init("C:\\spark-2.4.5-bin-hadoop2.7")
import pyspark
from pyspark.sql import SparkSession
from django.conf import settings
from django.core.files.storage import FileSystemStorage
from datetime import date
import json
import csv
import paramiko

ssh=paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('192.168.10.51', username='hadoop', password='hadoop')
spark=SparkSession.builder.appName('adjuster').getOrCreate()
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir=os.path.join(BASE_DIR,"data")
updated_ids =[]

def update_csv(request):
    global latest_file
    if "GET" == request.method:
        return HttpResponse({'message':"Invalid request."},status=500)

    #if not GET, then proceed
    try:
        # file_data = csv_file.read().decode("utf-8")
        ftp_client=ssh.open_sftp()
        latest_file=get_latest_file()
        ftp_client.get('/home/hadoop/data/'+latest_file,data_dir+"\\"+"latest.csv")
        list_of_files = glob.glob(data_dir+'\*')
        latest_file = max(list_of_files, key=os.path.getctime)
        print(latest_file)

        csv_files=request.FILES['csv_file']
        fs = FileSystemStorage(location=data_dir)
        filename = fs.save(csv_files.name, csv_files)
        file_url=data_dir+'\\'+fs.url(filename)
        print(file_url)

        data=update_entries(latest_file,file_url)
        data_filepath=data_dir+'\\'+str(date.today())+".csv"
        data.toPandas().to_csv(data_filepath,header=True,index=False)

        ftp_client.put(data_filepath,'/home/hadoop/data/'+str(date.today())+".csv")
        ftp_client.close()

        print("file read")
        os.remove(file_url)
        os.remove(data_filepath)
        os.remove(latest_file)
        return JsonResponse({'message':'File uploaded successfully'},status=200)
        print("test 1")
        return JsonResponse({'message':'File uploaded is not a '},status=415)
    except Exception as e:
        os.remove(file_url)
        print("test")
        logging.getLogger("error_logger").error("Unable to upload file. " + repr(e))
        messages.error(request, "Unable to upload file. " + repr(e))

    return JsonResponse({'message':"Error in uploading file."},status=500)
#from azure.storage.blob import BlobServiceClient
#from azure.storage.blob import ContainerClient
# spark=SparkSession.builder.appName('adjuster').getOrCreate()

#container_client = ContainerClient.from_connection_string(conn_str="DefaultEndpointsProtocol=https;AccountName=neha6767j;AccountKey=aJAQ0faLityhaj4RVNQ8UkQ1QiZmjwFHON09LwI+1t76dclfMV4ydwYe/ovTTvZfsc9Y4Isu3XoN9+A2RQK/0Q==;EndpointSuffix=core.windows.net"", container_name="my-container")

#container_client.create_container()


def add_csv(request):
    # sc = SparkContext(conf=conf)

    if "GET" == request.method:
        return HttpResponse({'message':"Invalid request."},status=500)

    #if not GET, then proceed
    try:
        ftp_client=ssh.open_sftp()
        latest_file=get_latest_file()
        ftp_client.get('/home/hadoop/data/'+latest_file,data_dir+"\\"+"latest.csv")
        list_of_files = glob.glob(data_dir+'\*')
        latest_file = max(list_of_files, key=os.path.getctime)
        print(latest_file)
        csv_files=request.FILES['csv_file']
        fs = FileSystemStorage(location=data_dir) #defaults to   MEDIA_ROOT
        filename = fs.save(csv_files.name, csv_files)
        file_url=data_dir+'\\'+fs.url(filename)
        print(file_url)
        data=add_entries(latest_file,file_url)
        data_filepath=data_dir+'\\'+str(date.today())+".csv"
        data.toPandas().to_csv(data_filepath,header=True,index=False)
        ftp_client.put(data_filepath,'/home/hadoop/data/'+str(date.today())+".csv")
        ftp_client.close()
        #data.coalesce(1).write.option('header','true').option("sep",",").mode("overwrite").csv('C:\\Users\\Administrator\\Desktop\\DataAdjustmentTool\\django\\data\\'+str(date.today())+'.csv')
        # put = subprocess.Popen(["hadoop", "fs", "-put",file_url,'hdfs://hadoop1.example.com:9000'], stdin=PIPE, bufsize=-1)
        # put.communicate()
        # hdfs=HDFileSystem(host='hdfs://hadoop1.example.com',port=9000)
        # print(hdfs)
            # handle_uploaded_file(request.FILES['csv_file'])
        # data=spark.read.csv(csv_files,inferSchema=True,header=True)
        # data.show()
        print("file read")
        os.remove(file_url)
        os.remove(data_filepath)
        os.remove(latest_file)
        return JsonResponse({'message':'File uploaded successfully'},status=200)
        print("test 1")
        return JsonResponse({'message':'File uploaded is not a '},status=415)
    except Exception as e:
        os.remove(file_url)
        print("test")
        logging.getLogger("error_logger").error("Unable to upload file. " + repr(e))
        messages.error(request, "Unable to upload file. " + repr(e))

    return JsonResponse({'message':"Error in uploading file."},status=500)


def download_csv(request):
    ftp_client=ssh.open_sftp()
    latest_file=get_latest_file()
    ftp_client.get('/home/hadoop/data/'+latest_file,data_dir+"\\"+"latest.csv")
    list_of_files = glob.glob(data_dir+'\*')
    latest_file = max(list_of_files, key=os.path.getctime)
    print(latest_file)
    df=spark.read.csv(latest_file,inferSchema=True,header=True)
    data={}
    print(updated_ids)
    for id in updated_ids:
        data[id]=df.filter(f"stud_id={id}").filter("latest=true").collect()[0].asDict()
    os.remove(data_dir+"\\"+"latest.csv")
    return JsonResponse(data,status=200)

def add_entries(data,add_file):
    global updated_ids
    updated_ids=[]
    new_rows=[]
    data=spark.read.csv(data,inferSchema=True,header=True)
    schema=data.schema
    results=data.agg({'stud_id':'count'})
    next_id=results.collect()[0].asDict()["count(stud_id)"]+1
    add_file=spark.read.csv(add_file,inferSchema=True,header=True)
    if "first_name" in add_file.columns and "middle_name" in add_file.columns and "last_name" in add_file.columns:
        add_file=add_file.collect()
        for i in range(len(add_file)):
            my_dict=add_file[i].asDict()
            sid=next_id
            updated_ids.append(sid)
            next_id+=1
            first_name=my_dict['first_name']
            middle_name=my_dict['middle_name']
            last_name=my_dict['last_name']
            valid_from=str(date.today())
            valid_to='till date'
            latest=True
            version_no=1
            new_rows.append((sid,first_name,middle_name,last_name,valid_from,valid_to,latest,version_no))
        if len(new_rows)>0:
            newRow = spark.createDataFrame(new_rows,schema)
            data = data.union(newRow)
    return data

def update_entries(data,update):
    global updated_ids
    updated_ids=[]
    data=spark.read.csv(data,inferSchema=True,header=True)
    schema=data.schema
    update=spark.read.csv(update,inferSchema=True,header=True)
    if "stud_id" in update.columns and "first_name" in update.columns and "middle_name" in update.columns and "last_name" in update.columns:
        update=update.collect()
        for i in range(0,len(update)):
            new_rows=[]
            update_dict=update[i].asDict()
            sid=update_dict['stud_id']
            updated_ids.append(sid)
            results=data.filter(f"stud_id={sid}").filter("latest=true")
            if len(results.collect())>0:
                old_dict=results.collect()[0].asDict()
                old_first_name=old_dict['first_name']
                old_middle_name=old_dict['middle_name']
                old_last_name=old_dict['last_name']
                old_valid_from=old_dict['valid_from']
                old_valid_to=str(date.today())
                old_version_no=old_dict['version_no']
                latest=False
                new_rows.append((sid,old_first_name,old_middle_name,old_last_name,old_valid_from,old_valid_to,latest,old_version_no))

                new_first_name=update_dict['first_name']
                new_middle_name=update_dict['middle_name']
                new_last_name=update_dict['last_name']
                new_valid_from=str(date.today())
                new_valid_to='till date'
                latest=True
                new_version_no=old_version_no+1
                new_rows.append((sid,new_first_name,new_middle_name,new_last_name,new_valid_from,new_valid_to,latest,new_version_no))
                data=data.union(results).subtract(data.intersect(results))
                if len(new_rows)>0:
                    newRow = spark.createDataFrame(new_rows,schema)
                    data = data.union(newRow)
    return data


def get_latest_file():
    ftp_client=ssh.open_sftp()
    ftp_client.chdir('/home/hadoop/data')
    latest=0
    for file_att in ftp_client.listdir_attr():
        if file_att.st_mtime>latest:
            latest=file_att.st_mtime
            latest_file=file_att.filename
    return latest_file