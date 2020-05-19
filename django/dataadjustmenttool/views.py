from django.shortcuts import render
from django.http import HttpResponse
from models import Student_details


@permission_required('admin.can_add_log_entry')
def upload_csv(request):
    template = "details_upload.html"
    prompt = {
        'order' : 'Order of csv should be SID, FIRST_NAME. MIDDLE_NAME, LAST_NAME, VLD_FROM, VLD_TILL'
    }
    if request.method == 'GET':
        return render(request, template, prompt)

    csv_file = request.FILES['change-input']

    if not csv_file.name.endswith('.csv'):
        messages.error(request, 'This is not a csv file.')

    data_set = csv_file.read().decode("utf-8")


    lines = data_set.split("\n")
    for line in lines:
        fields = line.split(",")
        data_dict = {"SID": fields[0], "FIRST_NAME": fields[1], "MIDDLE_NAME": fields[2], "LAST_NAME": fields[3],
                     "VLD_FROM": fields[4], "VLD_TILL": fields[5]
                     }
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
    logging.getLogger("error_logger").error("Unable to upload file. " + repr(e))
    messages.error(request, "Unable to upload file. " + repr(e))
    context = {}
    render(request, template, context)

