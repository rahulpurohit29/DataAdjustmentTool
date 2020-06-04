from django.views.generic import View
from django.shortcuts import render
from django.http import HttpResponse,JsonResponse
from django.contrib import messages
import logging
import os
import glob
from subprocess import Popen, PIPE
from pyspark import SparkContext, SparkConf
import findspark
findspark.init("C:\\spark-2.4.5-bin-hadoop2.7")
import pyspark
import tempfile
import subprocess
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
    global updated_ids
    updated_ids=[]
    if "GET" == request.method:
        return HttpResponse({'message':"Invalid request."},status=500)

    #if not GET, then proceed
    try:
        csv_files=request.FILES['csv_file']
        fs = FileSystemStorage(location=data_dir)
        filename = fs.save(csv_files.name, csv_files)
        file_url=data_dir+'\\'+fs.url(filename)
        print(file_url)
        updates=spark.read.csv(file_url,inferSchema=True,header=True)
        if "stud_id" in updates.columns and "first_name" in updates.columns and "middle_name" in updates.columns and "last_name" in updates.columns:
            ids=updates.select("stud_id").collect()
            for id in ids:
                id_dict=id.asDict()
                updated_ids.append(id_dict["stud_id"])

            ftp_client=ssh.open_sftp()
            ftp_client.put(file_url,'/home/hadoop/data/update.csv')
            ftp_client.close()
            stdin, stdout, stderr = ssh.exec_command('cd /hadoop/spark/bin \n ./spark-submit /home/hadoop/data/update.py \n cd /home/hadoop/data/final_data1 \n ls')
            print(stderr.read())
            updated_file=stdout.readlines()[0].split("\n")[0]
            stdin, stdout, stderr = ssh.exec_command('cp /home/hadoop/data/final_data1/'+updated_file+" /home/hadoop/data/final_data.csv")
            print(stderr.read())
            print("file read")
            os.remove(file_url)
            return JsonResponse({'message':'File updated successfully'},status=200)
        else:
            print("test 1")
            os.remove(file_url)
            return JsonResponse({'message':'File headers are incorrect'})
    except Exception as e:
        os.remove(file_url)
        print("test")
        logging.getLogger("error_logger").error("Unable to upload file. " + repr(e))
        messages.error(request, "Unable to upload file. " + repr(e))

    # return JsonResponse({'message':"Error in uploading file."},status=500)
def add_csv(request):
    global updated_ids
    updated_ids=[]

    if "GET" == request.method:
        return HttpResponse({'message':"Invalid request."},status=500)

    #if not GET, then proceed
    try:
        csv_files=request.FILES['csv_file']
        fs = FileSystemStorage(location=data_dir)
        filename = fs.save(csv_files.name, csv_files)
        file_url=data_dir+'\\'+fs.url(filename)
        print(file_url)

        additions=spark.read.csv(file_url,inferSchema=True,header=True)
        ftp_client=ssh.open_sftp()
        ftp_client.get('/home/hadoop/data/final_data.csv',data_dir+"\\"+"latest.csv")
        ftp_client.close()
        data=spark.read.csv(data_dir+"\\"+"latest.csv",inferSchema=True,header=True)
        schema=data.schema
        if "first_name" in data.columns and "middle_name" in data.columns and "last_name" in data.columns:
            results=data.agg({'stud_id':'count'})
            next_id=results.collect()[0].asDict()["count(stud_id)"]+1
            length=len(additions.collect())
            print(next_id,length)
            for i in range(next_id,next_id+length):
                updated_ids.append(i)
            print(updated_ids)
            os.remove(data_dir+"\\"+"latest.csv")
            
            ftp_client=ssh.open_sftp()
            ftp_client.put(file_url,'/home/hadoop/data/add.csv')
            ftp_client.close()

            stdin, stdout, stderr = ssh.exec_command('cd /hadoop/spark/bin \n ./spark-submit /home/hadoop/data/add.py \n cd /home/hadoop/data/final_data1 \n ls')
            updated_file=stdout.readlines()[0].split("\n")[0]
            stdin, stdout, stderr = ssh.exec_command('cp /home/hadoop/data/final_data1/'+updated_file+" /home/hadoop/data/final_data.csv")

            print("file read")
            os.remove(file_url)
            return JsonResponse({'message':'File added successfully'},status=200)
        else:
            print("test 1")
            os.remove(file_url)
            return JsonResponse({'message':'File headers are incorrect '})
    except Exception as e:
        os.remove(file_url)
        print("test")
        logging.getLogger("error_logger").error("Unable to upload file. " + repr(e))
        messages.error(request, "Unable to upload file. " + repr(e))

    return JsonResponse({'message':"Error in uploading file."},status=500)


def download_csv(request):
    ftp_client=ssh.open_sftp()

    ftp_client.get('/home/hadoop/data/final_data.csv',data_dir+"\\"+"latest.csv")
    df=spark.read.csv(data_dir+"\\"+"latest.csv",inferSchema=True,header=True)
    data={}
    print(updated_ids)
    for id in updated_ids:
        data[id]=df.filter(f"stud_id={id}").filter("latest=true").collect()[0].asDict()
    return JsonResponse(data,status=200)
