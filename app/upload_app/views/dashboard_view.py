# SAFE APPROACH: dashboard_view.py 
# This version doesn't import unknown classes, instead uses your original working approach
# with improvements for consistency

from django.shortcuts import render, redirect
from django.urls import reverse
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
import logging
from pymongo import MongoClient
from django.conf import settings
import re
from datetime import datetime

# Import your existing working API view classes
from .SlopeGeoJsonUploadView import GeoTiffUploadView  # Your working slope processor
from .country_adm_boundaries_upload_views import UploadShapefileCountryView  # Your working shapefile processor
from .malaria_htmis_api_upload_view import UploadHMISAPIDataView  # Your working HMIS processor
from .health_center_lab__data_upload_views import UploadHealthCenterLabDataView  # Your working health center processor


from ..forms import (
    ShapefileUploadForm,
    SlopeGeoJsonUploadForm,
    HealthCenterLabDataForm,
    HMISAPIDataForm,
    TemperatureDataForm,
    PrecipitationDataForm
)

logger = logging.getLogger(__name__)

# ==========================================
# MAIN DASHBOARD VIEWS (UI Controllers)
# ==========================================

@login_required
def upload_dashboard(request):
    """Main dashboard routing"""
    user = request.user
    is_admin = user.is_superuser or user.is_staff
    
    if is_admin:
        return redirect('upload_app:admin_dashboard')
    else:
        return redirect('upload_app:user_dashboard')

@login_required
def user_dashboard(request):
    """User dashboard with available forms"""
    user = request.user
    is_admin = user.is_superuser or user.is_staff
    
    context = {
        "user": user,
        "is_admin": is_admin,
        "username": user.username,
        "user_role": "User",
        
        # User-accessible forms
        "hc_form": HealthCenterLabDataForm(),
        "hmis_form": HMISAPIDataForm(),
        "temp_form": TemperatureDataForm(),
        "prec_form": PrecipitationDataForm(),
        
        "available_features": [
            "Health Center Malaria Records",
            "HMIS API Data", 
            "Temperature Data",
            "Precipitation Data"
        ],
        "restricted_features": [
            "Country Shapefile Upload (Admin Only)",
            "Slope GeoTIFF Upload (Admin Only)"
        ] if not is_admin else []
    }
    
    return render(request, "upload_app/user_dashboard.html", context)

@login_required  
def admin_dashboard(request):
    """Admin dashboard with all forms"""
    user = request.user
    is_admin = user.is_superuser or user.is_staff
    
    if not is_admin:
        return redirect('upload_app:user_dashboard')
    
    context = {
        "user": user,
        "is_admin": is_admin,
        "username": user.username,
        "user_role": "Administrator",
        
        # All forms for admin
        "shapefile_form": ShapefileUploadForm(),
        "slope_form": SlopeGeoJsonUploadForm(),
        "hc_form": HealthCenterLabDataForm(),
        "hmis_form": HMISAPIDataForm(),
        "temp_form": TemperatureDataForm(),
        "prec_form": PrecipitationDataForm(),
        
        "show_user_management": True,
        "show_data_management": True,
    }
    
    return render(request, "upload_app/admin_dashboard.html", context)

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def get_mongo_collection(collection_name):
    """Helper function to get MongoDB collection"""
    try:
        mongo_uri = getattr(settings, 'MONGO_URI')
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        db = client['weather_records_db']
        collection = db[collection_name]
        return client, collection
    except Exception as e:
        logger.error(f"MongoDB connection failed: {str(e)}")
        raise e

def create_collection_name(district, sector, year):
    """
    Create a standardized collection name based on district, sector, and year
    Format: healthcenter-data-{district}-{sector}-{year}
    SAME FUNCTION AS YOUR API VIEWS
    """
    # Clean and normalize the names (remove spaces, special chars, convert to lowercase)
    clean_district = re.sub(r'[^a-zA-Z0-9]', '', district.lower())
    clean_sector = re.sub(r'[^a-zA-Z0-9]', '', sector.lower())
    
    collection_name = f"healthcenter-data-{clean_district}-{clean_sector}-{year}"
    return collection_name

# ==========================================
# UPLOAD HANDLERS (SAFE IMPLEMENTATIONS)
# ==========================================
# # FIXED UPLOAD HANDLERS - Using correct field names from your forms
# # Replace the handlers in your dashboard_view.py with these


@login_required
@require_http_methods(["POST"])
def upload_slope_geojson(request):
    """
    BRIDGE HANDLER: Use your existing GeoTiffUploadView that does GeoTIFF->GeoJSON conversion
    """
    if not (request.user.is_superuser or request.user.is_staff):
        messages.error(request, "Admin access required")
        return redirect('upload_app:user_dashboard')
    
    logger.info("=== SLOPE GEOTIFF UPLOAD (USING API BRIDGE) ===")
    logger.info(f"Files received: {list(request.FILES.keys())}")
    
    try:
        # Get the uploaded file
        file = request.FILES.get('tif_file') or request.FILES.get('file')
        
        if not file:
            messages.error(request, "No slope data file was uploaded")
            return redirect('upload_app:admin_dashboard')
        
        # Create a mock API request for your existing GeoTiffUploadView
        class MockAPIRequest:
            def __init__(self, files, data, user):
                self.FILES = files
                self.data = data
                self.user = user
        
        # Prepare data for your API (matching your GeoTiffUploadView expectations)
        api_data = {
            'region': request.POST.get('region', 'Rwanda'),
            'year': request.POST.get('year_uploaded', datetime.now().year),
            'data_type': request.POST.get('data_type', 'slope'),
            'dataset_name': f"Slope Data - {request.POST.get('region', 'Rwanda')}",
            'description': request.POST.get('description', ''),
            'resolution': request.POST.get('resolution', ''),
            'source': request.POST.get('source', ''),
            'simplify_tolerance': '0.002',  # Good default for Rwanda
            'max_features': '8000',  # Reasonable limit
            'clip_to_rwanda': 'true',
            'add_admin_tags': 'false'  # Can be enabled later
        }
        
        # Create mock request
        mock_request = MockAPIRequest(
            files={'tif_file': file},
            data=api_data,
            user=request.user
        )
        
        # Use your existing, working GeoTiffUploadView
        slope_api_view = GeoTiffUploadView()
        api_response = slope_api_view.post(mock_request)
        
        # Convert API response to dashboard messages
        if api_response.status_code == 201:  # HTTP_201_CREATED
            response_data = api_response.data
            messages.success(
                request,
                f"✅ Successfully converted GeoTIFF to GeoJSON and stored in MongoDB! "
                f"Features: {response_data.get('features_count', 'Unknown')}"
            )
            logger.info(f"Slope GeoTIFF conversion successful: {response_data}")
        else:
            error_msg = api_response.data.get('error', 'Unknown error occurred')
            messages.error(request, f"❌ Slope data processing failed: {error_msg}")
            logger.error(f"Slope API error: {api_response.data}")
            
        return redirect('upload_app:admin_dashboard')
        
    except Exception as e:
        logger.error(f"Slope bridge handler failed: {str(e)}")
        messages.error(request, f"❌ Slope data upload failed: {str(e)}")
        return redirect('upload_app:admin_dashboard')

@login_required
@require_http_methods(["POST"])
def upload_country_shapefile(request):
    """
    BRIDGE HANDLER: Use your existing UploadShapefileCountryView with village detection
    """
    if not (request.user.is_superuser or request.user.is_staff):
        messages.error(request, "Admin access required")
        return redirect('upload_app:user_dashboard')
    
    logger.info("=== SHAPEFILE UPLOAD (USING API BRIDGE) ===")
    logger.info(f"Files received: {list(request.FILES.keys())}")
    
    try:
        file = request.FILES.get('file')
        
        if not file:
            messages.error(request, "No shapefile was uploaded")
            return redirect('upload_app:admin_dashboard')
        
        # Create mock API request for your existing UploadShapefileCountryView
        class MockAPIRequest:
            def __init__(self, files, data, user):
                self.FILES = files
                self.data = data
                self.user = user
        
        # Prepare data for your API (matching UploadShapefileCountryView expectations)
        api_data = {
            'dataset_name': request.POST.get('dataset_name', 'Rwanda Administrative Boundaries'),
            'country': request.POST.get('country', 'Rwanda'),
            'shapefile_type': 'villages',  # Your API specializes in village detection
            'year': request.POST.get('year', datetime.now().year),
            'source': request.POST.get('source', ''),
            'description': request.POST.get('description', '')
        }
        
        # Create mock request
        mock_request = MockAPIRequest(
            files={'file': file},
            data=api_data,
            user=request.user
        )
        
        # Use your existing, working UploadShapefileCountryView
        shapefile_api_view = UploadShapefileCountryView()
        api_response = shapefile_api_view.post(mock_request)
        
        # Convert API response to dashboard messages
        if api_response.status_code == 201:  # HTTP_201_CREATED
            response_data = api_response.data
            village_analysis = response_data.get('village_analysis', {})
            
            messages.success(
                request,
                f"✅ Successfully processed shapefile with village detection! "
                f"Found {village_analysis.get('total_villages', 'Unknown')} features. "
                f"Village name column: {village_analysis.get('village_name_column', 'Not identified')}"
            )
            
            # Show sample village names if available
            sample_names = village_analysis.get('sample_village_names', [])
            if sample_names:
                messages.info(
                    request,
                    f"📍 Sample villages: {', '.join(sample_names[:5])}"
                )
            
            logger.info(f"Shapefile processing successful: {response_data}")
        else:
            error_msg = api_response.data.get('error', 'Unknown error occurred')
            messages.error(request, f"❌ Shapefile processing failed: {error_msg}")
            logger.error(f"Shapefile API error: {api_response.data}")
            
        return redirect('upload_app:admin_dashboard')
        
    except Exception as e:
        logger.error(f"Shapefile bridge handler failed: {str(e)}")
        messages.error(request, f"❌ Shapefile upload failed: {str(e)}")
        return redirect('upload_app:admin_dashboard')

@login_required
@require_http_methods(["POST"])
def upload_hmis_records(request):
    """
    BRIDGE HANDLER: Use your existing UploadHMISAPIDataView
    """
    logger.info("=== HMIS UPLOAD (USING API BRIDGE) ===")
    logger.info(f"Files received: {list(request.FILES.keys())}")
    
    try:
        file = request.FILES.get('file')
        
        if not file:
            messages.error(request, "No HMIS file was uploaded")
            return redirect('upload_app:admin_dashboard')
        
        # Create mock API request for your existing UploadHMISAPIDataView
        class MockAPIRequest:
            def __init__(self, files, data, user):
                self.FILES = files
                self.data = data
                self.user = user
        
        # Prepare data for your API (matching UploadHMISAPIDataView expectations)
        api_data = {
            'dataset_name': request.POST.get('dataset_name', 'HMIS Dataset'),
            'district': request.POST.get('district', ''),
            'health_facility': request.POST.get('health_facility', ''),
            'year': request.POST.get('year', datetime.now().year),
            'data_period': request.POST.get('period', ''),
            'reporting_level': request.POST.get('reporting_level', 'facility'),
            'source': request.POST.get('source', 'HMIS Dashboard Upload'),
            'description': request.POST.get('description', '')
        }
        
        # Create mock request
        mock_request = MockAPIRequest(
            files={'file': file},
            data=api_data,
            user=request.user
        )
        
        # Use your existing, working UploadHMISAPIDataView
        hmis_api_view = UploadHMISAPIDataView()
        api_response = hmis_api_view.post(mock_request)
        
        # Convert API response to dashboard messages
        if api_response.status_code == 201:  # HTTP_201_CREATED
            response_data = api_response.data
            messages.success(
                request,
                f"✅ Successfully uploaded HMIS data! "
                f"Records: {response_data.get('records_inserted', 'Unknown')}"
            )
            
            # Show collection info
            collection_info = response_data.get('collection_info', {})
            if collection_info.get('data_collection'):
                messages.info(
                    request,
                    f"📁 Stored in collection: {collection_info['data_collection']}"
                )
            
            logger.info(f"HMIS processing successful: {response_data}")
        else:
            error_msg = api_response.data.get('error', 'Unknown error occurred')
            messages.error(request, f"❌ HMIS data processing failed: {error_msg}")
            logger.error(f"HMIS API error: {api_response.data}")
            
        return redirect('upload_app:admin_dashboard')
        
    except Exception as e:
        logger.error(f"HMIS bridge handler failed: {str(e)}")
        messages.error(request, f"❌ HMIS upload failed: {str(e)}")
        return redirect('upload_app:admin_dashboard')

@login_required
@require_http_methods(["POST"])
def upload_healthcenter_records(request):
    """
    BRIDGE HANDLER: Use your existing UploadHealthCenterLabDataView
    """
    logger.info("=== HEALTH CENTER UPLOAD (USING API BRIDGE) ===")
    logger.info(f"Files received: {list(request.FILES.keys())}")
    
    try:
        file = request.FILES.get('file')
        
        if not file:
            messages.error(request, "No health center file was uploaded")
            return redirect('upload_app:admin_dashboard')
        
        # Create mock API request for your existing UploadHealthCenterLabDataView
        class MockAPIRequest:
            def __init__(self, files, data, user):
                self.FILES = files
                self.data = data
                self.user = user
        
        # Prepare data for your API (matching UploadHealthCenterLabDataView expectations)
        api_data = {
            'dataset_name': request.POST.get('dataset_name', 'Health Center Lab Data'),
            'district': request.POST.get('district', ''),
            'sector': request.POST.get('sector', ''),
            'health_center': request.POST.get('health_center', ''),
            'year': request.POST.get('year', datetime.now().year)
        }
        
        # Create mock request
        mock_request = MockAPIRequest(
            files={'file': file},
            data=api_data,
            user=request.user
        )
        
        # Use your existing, working UploadHealthCenterLabDataView
        hc_api_view = UploadHealthCenterLabDataView()
        api_response = hc_api_view.post(mock_request)
        
        # Convert API response to dashboard messages
        if api_response.status_code == 201:  # HTTP_201_CREATED
            response_data = api_response.data
            messages.success(
                request,
                f"✅ Successfully uploaded health center data! "
                f"Records: {response_data.get('records_inserted', 'Unknown')}"
            )
            
            # Show collection info
            collection_info = response_data.get('collection_info', {})
            if collection_info.get('data_collection'):
                messages.info(
                    request,
                    f"📁 Stored in collection: {collection_info['data_collection']}"
                )
            
            logger.info(f"Health center processing successful: {response_data}")
        else:
            error_msg = api_response.data.get('error', 'Unknown error occurred')
            messages.error(request, f"❌ Health center data processing failed: {error_msg}")
            logger.error(f"Health center API error: {api_response.data}")
            
        return redirect('upload_app:admin_dashboard')
        
    except Exception as e:
        logger.error(f"Health center bridge handler failed: {str(e)}")
        messages.error(request, f"❌ Health center upload failed: {str(e)}")
        return redirect('upload_app:admin_dashboard')

# Keep your working temperature and precipitation handlers unchanged
# (They were already working, so no bridge needed)

# DIAGNOSTIC HANDLERS - Add these temporarily to debug the file field issue

@login_required
@require_http_methods(["POST"])
def upload_temperature_data(request):
    """
    DIAGNOSTIC HANDLER: Debug exactly what's happening with file uploads
    """
    logger.info("=== TEMPERATURE DIAGNOSTIC ===")
    logger.info(f"request.FILES: {dict(request.FILES)}")
    logger.info(f"request.FILES.keys(): {list(request.FILES.keys())}")
    logger.info(f"request.POST: {dict(request.POST)}")
    
    # Check all possible file field names
    possible_files = {
        'file': request.FILES.get('file'),
        'temperature': request.FILES.get('temperature'),
        'temperature_file': request.FILES.get('temperature_file'),
    }
    
    logger.info("=== FILE FIELD ANALYSIS ===")
    for field_name, file_obj in possible_files.items():
        if file_obj:
            logger.info(f"✅ Found file in field '{field_name}': {file_obj.name} (size: {file_obj.size})")
        else:
            logger.info(f"❌ No file in field '{field_name}'")
    
    # Find the actual file
    actual_file = None
    actual_field = None
    for field_name, file_obj in possible_files.items():
        if file_obj:
            actual_file = file_obj
            actual_field = field_name
            break
    
    if not actual_file:
        logger.error("No file found in any expected field")
        messages.error(request, "❌ No temperature file found in upload")
        return redirect('upload_app:admin_dashboard')
    
    logger.info(f"Using file from field: {actual_field}")
    
    try:
        # Test direct API call to your working view
        from .weather_data_prec_temp_upload_views import UploadTemperatureView
        
        class MockAPIRequest:
            def __init__(self, files, data, user):
                self.FILES = files
                self.data = data
                self.user = user
        
        # Extract form data
        station = request.POST.get('station', 'Test Station')
        years = request.POST.get('years', '2024')
        dataset_name = request.POST.get('dataset_name', 'Test Temperature Dataset')
        
        logger.info(f"Form data extracted: station={station}, years={years}, dataset_name={dataset_name}")
        
        # Prepare API request data
        api_data = {
            'station': station,
            'years': years,
            'dataset_name': dataset_name,
            'description': 'Uploaded via dashboard'
        }
        
        # Create mock request with the correct field name for your API
        mock_request = MockAPIRequest(
            files={'temperature': actual_file},  # Your API expects 'temperature' field
            data=api_data,
            user=request.user
        )
        
        logger.info("Calling UploadTemperatureView API...")
        
        # Call your working API
        temp_api_view = UploadTemperatureView()
        api_response = temp_api_view.post(mock_request)
        
        logger.info(f"API Response Status: {api_response.status_code}")
        logger.info(f"API Response Data: {api_response.data}")
        
        # Handle response
        if api_response.status_code == 201:
            response_data = api_response.data
            messages.success(
                request,
                f"✅ Temperature upload successful! Records: {response_data.get('records_inserted', 'Unknown')}"
            )
        else:
            messages.error(request, f"❌ API Error: {api_response.data.get('error', 'Unknown error')}")
    
    except Exception as e:
        logger.error(f"Temperature diagnostic failed: {str(e)}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        messages.error(request, f"❌ Temperature upload failed: {str(e)}")
    
    return redirect('upload_app:admin_dashboard')

@login_required
@require_http_methods(["POST"])
def upload_precipitation_data(request):
    """
    DIAGNOSTIC HANDLER: Debug exactly what's happening with file uploads
    """
    logger.info("=== PRECIPITATION DIAGNOSTIC ===")
    logger.info(f"request.FILES: {dict(request.FILES)}")
    logger.info(f"request.FILES.keys(): {list(request.FILES.keys())}")
    logger.info(f"request.POST: {dict(request.POST)}")
    
    # Check all possible file field names
    possible_files = {
        'file': request.FILES.get('file'),
        'precipitation': request.FILES.get('precipitation'),
        'precipitation_file': request.FILES.get('precipitation_file'),
    }
    
    logger.info("=== FILE FIELD ANALYSIS ===")
    for field_name, file_obj in possible_files.items():
        if file_obj:
            logger.info(f"✅ Found file in field '{field_name}': {file_obj.name} (size: {file_obj.size})")
        else:
            logger.info(f"❌ No file in field '{field_name}'")
    
    # Find the actual file
    actual_file = None
    actual_field = None
    for field_name, file_obj in possible_files.items():
        if file_obj:
            actual_file = file_obj
            actual_field = field_name
            break
    
    if not actual_file:
        logger.error("No file found in any expected field")
        messages.error(request, "❌ No precipitation file found in upload")
        return redirect('upload_app:admin_dashboard')
    
    logger.info(f"Using file from field: {actual_field}")
    
    try:
        # Test direct API call to your working view
        from .weather_data_prec_temp_upload_views import UploadPrecipitationView
        
        class MockAPIRequest:
            def __init__(self, files, data, user):
                self.FILES = files
                self.data = data
                self.user = user
        
        # Extract form data
        station = request.POST.get('station', 'Test Station')
        years = request.POST.get('years', '2024')
        dataset_name = request.POST.get('dataset_name', 'Test Precipitation Dataset')
        
        logger.info(f"Form data extracted: station={station}, years={years}, dataset_name={dataset_name}")
        
        # Prepare API request data
        api_data = {
            'station': station,
            'years': years,
            'dataset_name': dataset_name,
            'description': 'Uploaded via dashboard'
        }
        
        # Create mock request with the correct field name for your API
        mock_request = MockAPIRequest(
            files={'precipitation': actual_file},  # Your API expects 'precipitation' field
            data=api_data,
            user=request.user
        )
        
        logger.info("Calling UploadPrecipitationView API...")
        
        # Call your working API
        precip_api_view = UploadPrecipitationView()
        api_response = precip_api_view.post(mock_request)
        
        logger.info(f"API Response Status: {api_response.status_code}")
        logger.info(f"API Response Data: {api_response.data}")
        
        # Handle response
        if api_response.status_code == 201:
            response_data = api_response.data
            messages.success(
                request,
                f"✅ Precipitation upload successful! Records: {response_data.get('records_inserted', 'Unknown')}"
            )
        else:
            messages.error(request, f"❌ API Error: {api_response.data.get('error', 'Unknown error')}")
    
    except Exception as e:
        logger.error(f"Precipitation diagnostic failed: {str(e)}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        messages.error(request, f"❌ Precipitation upload failed: {str(e)}")
    
    return redirect('upload_app:admin_dashboard')


# ==========================================
# UTILITY VIEWS
# ==========================================

@login_required
def test_mongodb_connection(request):
    """Test MongoDB connection"""
    try:
        mongo_uri = getattr(settings, 'MONGO_URI')
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        
        db_names = client.list_database_names()
        client.close()
        
        return JsonResponse({
            'status': 'success',
            'message': 'MongoDB connection successful',
            'databases': db_names[:5]
        })
        
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'error': str(e)
        })

@login_required
def view_uploaded_data(request):
    """View summary of uploaded data"""
    try:
        mongo_uri = getattr(settings, 'MONGO_URI')
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        
        # Check both weather_records_db and other possible databases
        databases_to_check = [
            'weather_records_db',
            getattr(settings, 'MONGO_DB', None)
        ]
        
        all_collections_info = {}
        total_records = 0
        
        for db_name in databases_to_check:
            if not db_name:
                continue
                
            try:
                db = client[db_name]
                collection_names = db.list_collection_names()
                
                for coll_name in collection_names:
                    try:
                        collection = db[coll_name]
                        count = collection.count_documents({})
                        all_collections_info[f"{db_name}.{coll_name}"] = count
                        total_records += count
                    except:
                        all_collections_info[f"{db_name}.{coll_name}"] = 0
            except:
                continue
        
        client.close()
        
        return JsonResponse({
            'status': 'success',
            'data_summary': all_collections_info,
            'total_records': total_records,
            'databases_checked': [db for db in databases_to_check if db]
        })
        
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'error': str(e)
        })