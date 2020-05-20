from django.conf.urls import url
from adjuster import views
from views import upload_csv

urlpatterns = [
    url(r'^adjuster',include('adjuster.urls)),
]
