from django.db import models

# Create your models here.

class Student_details(models.Model):
    SID = models.CharField(max_length = 50)
    FIRST_NAME = models.CharField(max_length = 50)
    MIDDLE_NAME = models.CharField(max_length = 50)
    LAST_NAME = models.CharField(max_length = 50)
    VLD_FROM = models.DateField(auto_now=False, auto_now_add=True)
    VLD_TILL = models.DateField(auto_now=True, auto_now_add=False)
