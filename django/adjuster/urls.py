from django.conf.urls import url,include
from django.urls import path
from adjuster import views
from views import upload_csv, download_csv

urlpatterns = [
    url(r'^adjuster',include('adjuster.urls')),
    path('upload_csv', views.upload_csv),
    path('export/csv-database-write/', views.csv_database_write, name='csv_database_write'),
]
