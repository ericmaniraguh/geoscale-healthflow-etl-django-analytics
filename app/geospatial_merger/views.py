# =============================================================================
# UPDATED VIEWS WITH REAL PROGRESS TRACKING
# File: app/geospatial_merger/views.py
# =============================================================================

import os
import json
import tempfile
import uuid
import threading
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required, user_passes_test
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.contrib.auth import logout
from pymongo import MongoClient
from .processors.batch_processor import GeospatialBatchProcessor

def is_admin(user):
    return user.is_authenticated and (user.is_staff or user.is_superuser)

@login_required
def dashboard(request):
    if not is_admin(request.user):
        return redirect('geospatial_merger:access_denied')
    
    context = {
        'user': request.user,
        'is_admin': True,
        'page_title': 'Geospatial Data Merger Dashboard'
    }
    return render(request, 'geospatial_merger/dashboard.html', context)

@login_required
def access_denied_view(request):
    return render(request, 'geospatial_merger/access_denied.html', {
        'message': 'Administrator privileges required.',
        'login_url': '/auth/login/',
        'admin_url': '/admin/',
        'user': request.user if request.user.is_authenticated else None
    })

@csrf_exempt
@user_passes_test(is_admin)
def upload_files(request):
    if request.method != "POST":
        return JsonResponse({"success": False, "message": "POST method required"})
    
    geojson_file = request.FILES.get("geojson")
    geotiff_file = request.FILES.get("geotiff")
    
    if not geojson_file or not geotiff_file:
        return JsonResponse({"success": False, "message": "Both files required"})
    
    try:
        process_id = str(uuid.uuid4())
        temp_dir = tempfile.mkdtemp(prefix=f"geospatial_{process_id}_")
        
        # Save files
        geojson_path = save_file(geojson_file, temp_dir, "boundaries")
        geotiff_path = save_file(geotiff_file, temp_dir, "slope")
        
        # Store in session
        request.session[f"geojson_path_{process_id}"] = geojson_path
        request.session[f"geotiff_path_{process_id}"] = geotiff_path
        request.session[f"temp_dir_{process_id}"] = temp_dir
        
        return JsonResponse({
            "success": True, 
            "process_id": process_id,
            "message": "Files uploaded successfully"
        })
        
    except Exception as e:
        return JsonResponse({"success": False, "message": str(e)})

def save_file(file, temp_dir, prefix):
    filename = f"{prefix}_{file.name}"
    filepath = os.path.join(temp_dir, filename)
    
    with open(filepath, 'wb') as destination:
        for chunk in file.chunks():
            destination.write(chunk)
    
    return filepath

@user_passes_test(is_admin)
@require_http_methods(["POST"])
def start_merge_process(request):
    process_id = request.POST.get('process_id')
    
    if not process_id:
        return JsonResponse({"success": False, "message": "Process ID required"})
    
    geojson_path = request.session.get(f"geojson_path_{process_id}")
    geotiff_path = request.session.get(f"geotiff_path_{process_id}")
    
    if not geojson_path or not geotiff_path:
        return JsonResponse({"success": False, "message": "File paths not found"})
    
    # Start processing in background
    def process_in_background():
        try:
            processor = GeospatialBatchProcessor(process_id)
            processor.process_files(geojson_path, geotiff_path)
        except Exception as e:
            print(f"Background processing error: {e}")
    
    thread = threading.Thread(target=process_in_background)
    thread.daemon = True
    thread.start()
    
    return JsonResponse({"success": True, "message": "Processing started"})

@user_passes_test(is_admin)
@require_http_methods(["GET"])
def get_processing_status(request):
    """Get real processing status with Windows-compatible file paths"""
    process_id = request.GET.get('process_id')
    
    if not process_id:
        return JsonResponse({"error": "Process ID required"}, status=400)
    
    # Try multiple possible progress file locations
    temp_base = tempfile.gettempdir()
    progress_files = [
        os.path.join(temp_base, f"progress_{process_id}.json"),
        os.path.join(os.getcwd(), f"progress_{process_id}.json"),
        f"progress_{process_id}.json"
    ]
    
    for progress_file in progress_files:
        try:
            if os.path.exists(progress_file):
                with open(progress_file, 'r') as f:
                    status = json.load(f)
                return JsonResponse(status)
        except Exception as e:
            continue
    
    # No progress file found
    return JsonResponse({
        "stage": "initializing",
        "progress": 0,
        "message": "Initializing...",
        "completed": False
    })

@user_passes_test(is_admin)
@require_http_methods(["GET"])
def get_results_preview(request):
    """Get preview with Windows-compatible file paths"""
    process_id = request.GET.get('process_id')
    
    if not process_id:
        return JsonResponse({"error": "Process ID required"}, status=400)
    
    # Try MongoDB first
    try:
        client = MongoClient(os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'), 
                           serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[os.getenv('MONGODB_DB', 'geospatial_db')]
        collection = db['processed_data']
        
        results = list(collection.find(
            {"process_id": process_id},
            {"_id": 0, "geometry": 0}
        ).limit(5))
        
        if results:
            return JsonResponse({"preview": results})
        
    except Exception as e:
        print(f"MongoDB preview failed: {e}")
    
    # Fallback to file
    temp_base = tempfile.gettempdir()
    results_files = [
        os.path.join(temp_base, f"results_{process_id}.json"),
        os.path.join(os.getcwd(), f"results_{process_id}.json"),
        f"results_{process_id}.json"
    ]
    
    for results_file in results_files:
        try:
            if os.path.exists(results_file):
                with open(results_file, 'r') as f:
                    all_results = json.load(f)
                
                # Return first 5 without geometry
                preview = []
                for result in all_results[:5]:
                    preview_item = {k: v for k, v in result.items() if k != 'geometry'}
                    preview.append(preview_item)
                
                return JsonResponse({"preview": preview})
                
        except Exception as e:
            continue
    
    return JsonResponse({"preview": []})

@user_passes_test(is_admin)
@require_http_methods(["GET"])
def export_merged_data(request):
    """Export with Windows-compatible file paths"""
    process_id = request.GET.get('process_id')
    
    if not process_id:
        return JsonResponse({"error": "Process ID required"}, status=400)
    
    # Try multiple possible GeoJSON file locations
    temp_base = tempfile.gettempdir()
    geojson_files = [
        os.path.join(temp_base, f"villages_slope_{process_id}.geojson"),
        os.path.join(os.getcwd(), f"villages_slope_{process_id}.geojson"),
        f"villages_slope_{process_id}.geojson"
    ]
    
    for geojson_file in geojson_files:
        if os.path.exists(geojson_file):
            try:
                with open(geojson_file, 'r', encoding='utf-8') as f:
                    geojson_content = f.read()
                
                response = HttpResponse(geojson_content, content_type='application/json')
                response['Content-Disposition'] = f'attachment; filename="villages_slope_{process_id}.geojson"'
                return response
                
            except Exception as e:
                print(f"Error reading GeoJSON file {geojson_file}: {e}")
                continue
    
    return JsonResponse({
        "error": "No results file found. Processing may not be complete or failed.", 
        "process_id": process_id
    }, status=404)



@login_required
def check_admin_status(request):
    return JsonResponse({"is_admin": is_admin(request.user)})

@login_required
@csrf_exempt
def logout_user(request):
    if request.method == "POST":
        logout(request)
        return JsonResponse({"success": True, "redirect": "/auth/login/"})
    return JsonResponse({"success": False})