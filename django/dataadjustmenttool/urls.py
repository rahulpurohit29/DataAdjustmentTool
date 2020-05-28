"""dataadjustmenttool URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.views.decorators.csrf import csrf_exempt
from django.urls import path
from django.conf.urls import url,include
from adjuster.views import update_csv,add_csv,download_csv

urlpatterns = [
    path('update_csv', csrf_exempt(update_csv)),
    path('add_csv',csrf_exempt(add_csv)),
    path('download_csv', csrf_exempt(download_csv)),
    url('admin/', admin.site.urls),
]
