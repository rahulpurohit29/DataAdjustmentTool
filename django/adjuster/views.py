from django.views.generic import View
from django.shortcuts import render
from django.http import HttpResponse
from adjuster import models
from subprocess import Popen, PIPE
from hdfs3 import HDFileSystem
from pyspark import SparkContext, SparkConf


def upload_csv(request):
    conf = SparkConf().setAppName("adjuster").setMaster("http://localhost:9000")
    sc = SparkContext(conf=conf)
    data = {}
    if "GET" == request.method:
        return render(request, "myapp/upload_csv.html", data)
    # if not GET, then proceed
    try:
        csv_file = StudentForm(request.POST, request.FILES)
        file_data = csv_file.read().decode("utf-8")
        if student.is_valid():
            handle_uploaded_file(request.FILES['csv_file'])
            spark.read.format('csv').options(header='true', inferschema='true').load(csv_file.csv)
            return HttpResponse({'message':"File uploaded successfully"},status=200)

    except Exception as e:
        logging.getLogger("error_logger").error("Unable to upload file. " + repr(e))
        messages.error(request, "Unable to upload file. " + repr(e))

    return HttpResponseRedirect(reverse("upload_csv"))


def download_csv(request):
    conf = SparkConf().setAppName(adjuster).setMaster("http://localhost:9000")
    sc = SparkContext(conf=conf)
    # getting data from hdfs
    details = data.objects.all()
    # Create the HttpResponse object with the appropriate CSV header.

    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="csv_database_write.csv" '

    writer = csv.writer(response)
    writer.writerow(['stud_id', 'first_name', 'middle_name', 'last_name', 'valid_from', 'valid_to'])
    for detail in details:
        writer.writerow([detail.stud_id, detail.first_name, detail.middle_name, detail.last_name, detail.valid_from,
                         detail.valid_to])

    return response
