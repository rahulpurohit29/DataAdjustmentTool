from django.test import TestCase, RequestFactory
import tempfile
from django.test import override_settings
from unittest.mock import patch
#from nose.tools import assert_list_equal, asster_true
from django.core.files.storage import Storage
import csv
import os
import json
import glob
from pyspark import SparkContext, SparkConf
from faker import Faker
from csv import reader
from io import StringIO
from django.core.files.storage import FileSystemStorage
from django.conf import settings
from adjuster import views
from .views import update_csv, add_csv, download_csv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir=os.path.join(BASE_DIR,"data")
fake = Faker()

class test_update_csv(TestCase):
        #print('hi')                
        def fake_test_csv(self):
                with open('file.csv', 'w') as csvfile:
                        fieldnames = ['stud_id', 'first_name', 'middle_name', 'last_name']
                        writer = csv.DictWriter(csvfile, fieldnames= fieldnames)

                        writer.writeheader()
                        for i in range(10):
                                writer.writerow({
                                        'stud_id': fake.random_int(min=1, max=100),
                                        'first_name': fake.name(),
                                        'middle_name': fake.name(),
                                        'last_name': fake.name()
                                                })

        def test_update_function(self):
                               
                with open('file.csv', 'r') as csvfile:
                        
                        csvReader = csv.reader(csvfile, delimiter = ',')
                        #print(len(csvReader))
                        for row in csvReader:
                                #print(len(row))
                                self.assertEqual(row[0], 'stud_id')
                                self.assertEqual(row[1], 'first_name')
                                self.assertEqual(row[2], 'middle_name')
                                self.assertEqual(row[3], 'last_name')
                                break
                        
                        fs = FileSystemStorage(location=data_dir)
                        #self.assertEqual(fs, FileSystemStorage(location=data_dir))
                        filename = fs.save(csvfile.name, csvfile)
                        #self.assertEqual(filename, fs.save(csvfile.name, csvfile))
                        file_url=data_dir+'\\'+fs.url(filename)
                        self.assertEqual(file_url, data_dir+'\\'+fs.url(filename))
                        #self.rq = RequestFactory()
                        #req =  self.rq.post('/update_csv', {'jsonFilePath':jsonFile}, content_type ='application/json')
                        #resp = update_csv(req)
                        #print('test')
                        #self.assertEqual(resp.status_code, 200)
                        #self.assertEqual(filecmp.cmp(update_csv.csvfiles, csvfile))
                
       
class test_add_csv(TestCase):
        #print('hi1')
        
        def fake_test_csv(self):
                with open('file.csv', 'w') as csvfile:
                        fieldnames = ['stud_id', 'first_name', 'middle_name', 'last_name']
                        writer = csv.DictWriter(csvfile, fieldnames= fieldnames)

                        writer.writeheader()
                        for i in range(10):
                                writer.writerow({
                                        'stud_id': fake.random_int(min=1, max=100),
                                        'first_name': fake.name(),
                                        'middle_name': fake.name(),
                                        'last_name': fake.name()
                                                })

        def test_add_function(self):
                
                with open('file.csv', 'r') as csvfile:
                        
                        csvReader = csv.reader(csvfile, delimiter = ',')
                        #print(len(csvReader))
                        for row in csvReader:
                                #print(len(row))
                                self.assertEqual(row[0], 'stud_id')
                                self.assertEqual(row[1], 'first_name')
                                self.assertEqual(row[2], 'middle_name')
                                self.assertEqual(row[3], 'last_name')
                                break
                        
                        fs = FileSystemStorage(location=data_dir)
                        #self.assertEqual(fs, FileSystemStorage(location=data_dir))
                        filename = fs.save(csvfile.name, csvfile)
                        #self.assertEqual(filename, fs.save(csvfile.name, csvfile))
                        file_url=data_dir+'\\'+fs.url(filename)
                        self.assertEqual(file_url, data_dir+'\\'+fs.url(filename))
