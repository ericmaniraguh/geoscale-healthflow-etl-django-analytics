# Create this new file: app/upload_app/handlers/upload_handlers.py

from django.shortcuts import redirect
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_http_methods
import logging
from ..forms import HealthCenterLabDataForm
from ..views.health_center_lab__data_upload_views import YourHealthCenterViewClass  # Replace with actual class
from ..forms import SlopeGeoJsonUploadForm
from ..views.SlopeGeoJsonUploadView import YourSlopeViewClass  # Replace with actual class name
from ..forms import PrecipitationDataForm
from ..views.weather_data_prec_temp_upload_views import YourPrecipitationViewClass  # Replace with actual class
from ..forms import ShapefileUploadForm
from ..views.country_adm_boundaries_upload_views import UploadShapefileView


logger = logging.getLogger(__name__)

class UploadRedirectHandler:
    """
    Centralized upload handler that processes uploads and redirects users properly.
    This eliminates the DRF browsable API interface issue.
    """
    
    @staticmethod
    def _get_redirect_url(user, success=True):
        """Determine where to redirect user based on their role"""
        if user.is_superuser or user.is_staff:
            return 'upload_app:admin_dashboard'
        else:
            return 'upload_app:user_dashboard'
    
    @staticmethod
    def _handle_form_errors(request, form):
        """Process and display form validation errors"""
        for field, errors in form.errors.items():
            for error in errors:
                messages.error(request, f"{field}: {error}")
    
    @staticmethod
    def _process_upload_response(request, response, file_name, upload_type):
        """Process response from existing view and set appropriate messages"""
        try:
            if hasattr(response, 'status_code'):
                if response.status_code == 201:  # Success
                    messages.success(
                        request, 
                        f"Successfully uploaded {upload_type}: {file_name}"
                    )
                    return True
                elif response.status_code >= 400:  # Error
                    error_data = getattr(response, 'data', {})
                    error_msg = error_data.get('error', 'Upload processing failed')
                    messages.error(request, f"Upload failed: {error_msg}")
                    return False
                else:
                    messages.info(request, f"{upload_type} upload processed")
                    return True
            else:
                messages.success(request, f"Successfully processed {upload_type}")
                return True
                
        except Exception as e:
            logger.error(f"Error processing upload response: {str(e)}")
            messages.error(request, f"Upload failed: {str(e)}")
            return False

# Individual handler functions that use existing view classes
@login_required
@require_http_methods(["POST"])
def handle_shapefile_upload(request):
    """Handle country shapefile uploads with redirect"""
    handler = UploadRedirectHandler()
    
    # Check admin permissions
    if not (request.user.is_superuser or request.user.is_staff):
        messages.error(request, "Access denied. Administrator privileges required.")
        return redirect(handler._get_redirect_url(request.user, False))
    

    form = ShapefileUploadForm(request.POST, request.FILES)
    
    if form.is_valid():
        try:
            file = request.FILES.get('file')
            
            # Use existing view class
            view_instance = UploadShapefileView()
            view_instance.request = request
            response = view_instance.post(request)
            
            handler._process_upload_response(request, response, file.name, "shapefile")
            
        except Exception as e:
            logger.error(f"Shapefile upload error: {str(e)}")
            messages.error(request, f"Upload failed: {str(e)}")
    else:
        handler._handle_form_errors(request, form)
    
    return redirect(handler._get_redirect_url(request.user))

@login_required
@require_http_methods(["POST"])
def handle_slope_upload(request):
    """Handle slope data uploads with redirect"""
    handler = UploadRedirectHandler()
    
    # Check admin permissions
    if not (request.user.is_superuser or request.user.is_staff):
        messages.error(request, "Access denied. Administrator privileges required.")
        return redirect(handler._get_redirect_url(request.user, False))
    
 
    form = SlopeGeoJsonUploadForm(request.POST, request.FILES)
    
    if form.is_valid():
        try:
            file = request.FILES.get('tif_file')
            
            # Use existing view class
            view_instance = YourSlopeViewClass()
            view_instance.request = request
            response = view_instance.post(request)
            
            handler._process_upload_response(request, response, file.name, "slope data")
            
        except Exception as e:
            logger.error(f"Slope upload error: {str(e)}")
            messages.error(request, f"Upload failed: {str(e)}")
    else:
        handler._handle_form_errors(request, form)
    
    return redirect(handler._get_redirect_url(request.user))


@login_required
@require_http_methods(["POST"])
def handle_healthcenter_upload(request):
    """Handle health center records upload with redirect"""
    handler = UploadRedirectHandler()
    
    from ..forms import HealthCenterLabDataForm
    from ..views.health_center_lab__data_upload_views import YourHealthCenterViewClass
    
    form = HealthCenterLabDataForm(request.POST, request.FILES)
    
    if form.is_valid():
        try:
            file = request.FILES.get('file')
            view_instance = YourHealthCenterViewClass()
            view_instance.request = request
            response = view_instance.post(request)
            
            handler._process_upload_response(
                request, response, file.name, "health center records"
            )
        except Exception as e:
            logger.error(f"Health center upload error: {str(e)}")
            messages.error(request, f"Upload failed: {str(e)}")
    else:
        handler._handle_form_errors(request, form)
    
    return redirect(handler._get_redirect_url(request.user))


@login_required
@require_http_methods(["POST"])
def handle_hmis_upload(request):
    """Handle HMIS records upload with redirect"""
    handler = UploadRedirectHandler()
    
    from ..forms import HMISAPIDataForm
    from ..views.malaria_htmis_api_upload_view import YourHMISViewClass  # Replace with actual class
    
    form = HMISAPIDataForm(request.POST, request.FILES)
    
    if form.is_valid():
        try:
            file = request.FILES.get('file')
            
            # Use existing view class
            view_instance = YourHMISViewClass()
            view_instance.request = request
            response = view_instance.post(request)
            
            handler._process_upload_response(request, response, file.name, "HMIS records")
            
        except Exception as e:
            logger.error(f"HMIS upload error: {str(e)}")
            messages.error(request, f"Upload failed: {str(e)}")
    else:
        handler._handle_form_errors(request, form)
    
    return redirect(handler._get_redirect_url(request.user))

@login_required
@require_http_methods(["POST"])
def handle_temperature_upload(request):
    """Handle temperature data upload with redirect"""
    handler = UploadRedirectHandler()
    
    from ..forms import TemperatureDataForm
    from ..views.weather_data_prec_temp_upload_views import YourTemperatureViewClass  # Replace with actual class
    
    form = TemperatureDataForm(request.POST, request.FILES)
    
    if form.is_valid():
        try:
            file = request.FILES.get('temperature')
            
            # Use existing view class
            view_instance = YourTemperatureViewClass()
            view_instance.request = request
            response = view_instance.post(request)
            
            handler._process_upload_response(request, response, file.name, "temperature data")
            
        except Exception as e:
            logger.error(f"Temperature upload error: {str(e)}")
            messages.error(request, f"Upload failed: {str(e)}")
    else:
        handler._handle_form_errors(request, form)
    
    return redirect(handler._get_redirect_url(request.user))

@login_required
@require_http_methods(["POST"])
def handle_precipitation_upload(request):
    """Handle precipitation data upload with redirect"""
    handler = UploadRedirectHandler()
    

    form = PrecipitationDataForm(request.POST, request.FILES)
    
    if form.is_valid():
        try:
            file = request.FILES.get('precipitation')
            
            # Use existing view class
            view_instance = YourPrecipitationViewClass()
            view_instance.request = request
            response = view_instance.post(request)
            
            handler._process_upload_response(request, response, file.name, "precipitation data")
            
        except Exception as e:
            logger.error(f"Precipitation upload error: {str(e)}")
            messages.error(request, f"Upload failed: {str(e)}")
    else:
        handler._handle_form_errors(request, form)
    
    return redirect(handler._get_redirect_url(request.user))