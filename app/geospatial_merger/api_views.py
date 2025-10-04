import os, tempfile
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .processors import SlopeBoundariesProcessor

progress_tracker = {"progress": 0, "status": "Idle"}

@csrf_exempt
def upload_files(request):
    global progress_tracker
    if request.method == "POST":
        try:
            geotiff = request.FILES.get("geotiff")
            zipfile = request.FILES.get("zip")
            if not geotiff or not zipfile:
                return JsonResponse({"success": False, "message": "Both files required"})

            tmp_dir = tempfile.mkdtemp()
            geotiff_path = os.path.join(tmp_dir, geotiff.name)
            zip_path = os.path.join(tmp_dir, zipfile.name)

            with open(geotiff_path, "wb+") as dest:
                for chunk in geotiff.chunks():
                    dest.write(chunk)
            with open(zip_path, "wb+") as dest:
                for chunk in zipfile.chunks():
                    dest.write(chunk)

            processor = SlopeBoundariesProcessor(progress_tracker)
            processor.process_in_batches(geotiff_path, zip_path, batch_size=20)

            return JsonResponse({"success": True})
        except Exception as e:
            return JsonResponse({"success": False, "message": str(e)})
    return JsonResponse({"success": False, "message": "POST only allowed"})

def get_processing_status(request):
    global progress_tracker
    return JsonResponse(progress_tracker)

