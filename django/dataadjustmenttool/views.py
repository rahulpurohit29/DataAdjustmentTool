from django.views.generic import View
from django.shortcuts import render
from django.http import HttpResponse
from dataadjustmenttool import models


def upload_csv(request):
	data = {}
	if "GET" == request.method:
		return render(request, "myapp/upload_csv.html", data)
    # if not GET, then proceed
	try:
		csv_file = request.FILES["csv_file"]
		file_data = csv_file.read().decode("utf-8")		

		lines = file_data.split("\n")
		#loop over the lines and save them in db. If error , store as string and then display
		for line in lines:						
			fields = line.split(",")
			data_dict = {"SID": fields[0], "FIRST_NAME": fields[1], "MIDDLE_NAME": fields[2], "LAST_NAME": fields[3],
                     "VLD_FROM": fields[4], "VLD_TILL": fields[5]
                     }

			print(data_dict)
			try:
				form = EventsForm(data_dict)
				if form.is_valid():
					form.save()					
				else:
					logging.getLogger("error_logger").error(form.errors.as_json())												
			except Exception as e:
				logging.getLogger("error_logger").error(repr(e))					
				pass

	except Exception as e:
		logging.getLogger("error_logger").error("Unable to upload file. "+repr(e))
		messages.error(request,"Unable to upload file. "+repr(e))

	return HttpResponseRedirect(reverse("upload_csv"))
'''
class FrontendRenderView(View):
        def get(self, request, *args, **kwargs):
                return render(request, "angular/src/app/apps.component.html", {}) '''
