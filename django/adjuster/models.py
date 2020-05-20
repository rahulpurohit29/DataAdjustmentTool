from django.db import models

# Create your models here.

class Student_details(models.Model):
    stud_id = models.CharField(max_length = 50)
    first_name = models.CharField(max_length = 50)
    middle_name = models.CharField(max_length = 50)
    last_name = models.CharField(max_length = 50)
    valid_from = models.DateField(auto_now=False, auto_now_add=True)
    valid_to = models.DateField(auto_now=True, auto_now_add=False)
