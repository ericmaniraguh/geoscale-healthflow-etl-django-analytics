
# etl_app/views/etl_dashboard_view.py - Complete version with AJAX support
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.http import JsonResponse
import logging
import requests
import json
from datetime import datetime

logger = logging.getLogger(__name__)

@login_required
def etl_dashboard(request):
    """Main ETL Dashboard - handles both display and form submissions"""
    
    # Get base URL dynamically
    base_url = request.build_absolute_uri('/').rstrip('/')
    
    if request.method == 'POST':
        # Check if it's an AJAX request
        is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        
        result = handle_form_submission(request, base_url, is_ajax)
        return result
    
    # GET request - show dashboard
    context = {
        "user": request.user,
        "username": request.user.username,
        "user_role": "Admin" if (request.user.is_superuser or request.user.is_staff) else "Data Processor",
        "is_admin": request.user.is_superuser or request.user.is_staff,
        
        # ETL Statistics
        "etl_stats": {
            "total_etl_processes": 5,
            "last_run": datetime.now().strftime('%Y-%m-%d %H:%M'),
            "status": "All Systems Operational"
        }
    }
    
    return render(request, "etl_app/etl_dashboard.html", context)

def detect_form_type(request):
    """Detect form type based on POST data fields - More robust detection"""
    post_data = dict(request.POST)
    
    # Remove common fields
    common_fields = ['csrfmiddlewaretoken', 'save_to_postgres', 'show_available']
    field_names = set(post_data.keys()) - set(common_fields)
    
    logger.info(f"Unique form fields: {field_names}")
    
    # Check for submit button names first (most reliable)
    if 'malaria_submit' in post_data:
        return 'malaria'
    elif 'health_center_submit' in post_data:
        return 'health_center'
    elif 'weather_prec_temp_submit' in post_data:
        return 'weather'
    elif 'boundaries_submit' in post_data:
        return 'boundaries'
    elif 'slope_submit' in post_data:
        return 'slope'
    
    # Fallback: Detect by unique field combinations
    if 'province' in field_names and 'district' in field_names and 'years' in field_names:
        return 'malaria'
    elif 'station_temp' in field_names or 'station_prec' in field_names:
        return 'weather'
    elif 'extraction_type' in field_names or 'min_lon' in field_names:
        return 'slope'
    elif 'transform_coords' in post_data or 'update_mode' in post_data:
        return 'boundaries'
    elif 'district' in field_names and 'sector' in field_names and 'years' in field_names:
        return 'health_center'
    
    return None

def handle_form_submission(request, base_url, is_ajax=False):
    """Handle all ETL form submissions with improved detection"""
    logger.info("=== ETL FORM SUBMISSION ===")
    logger.info(f"POST data: {dict(request.POST)}")
    logger.info(f"Is AJAX: {is_ajax}")
    
    try:
        # Detect form type using improved logic
        form_type = detect_form_type(request)
        logger.info(f"Detected form type: {form_type}")
        
        if form_type == 'malaria':
            return process_malaria_etl(request, base_url, is_ajax)
        elif form_type == 'health_center':
            return process_health_center_etl(request, base_url, is_ajax)
        elif form_type == 'weather':
            return process_weather_etl(request, base_url, is_ajax)
        elif form_type == 'boundaries':
            return process_boundaries_etl(request, base_url, is_ajax)
        elif form_type == 'slope':
            return process_slope_etl(request, base_url, is_ajax)
        else:
            # More detailed error message
            available_fields = list(request.POST.keys())
            error_msg = f"Could not identify form type. Available fields: {', '.join(available_fields)}"
            logger.error(error_msg)
            
            if is_ajax:
                return JsonResponse({'success': False, 'error': error_msg}, status=400)
            else:
                messages.error(request, error_msg)
                return redirect('etl_app:etl_dashboard')
            
    except Exception as e:
        logger.error(f"Error in form submission: {str(e)}", exc_info=True)
        
        if is_ajax:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
        else:
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('etl_app:etl_dashboard')

def process_malaria_etl(request, base_url, is_ajax=False):
    """Process Malaria API Calculator ETL"""
    logger.info("=== MALARIA ETL PROCESSING ===")
    
    try:
        # Parse years string to array
        years_str = request.POST.get('years', '').strip()
        if not years_str:
            raise ValueError("Years field is required")
        
        try:
            years = [int(year.strip()) for year in years_str.split(',') if year.strip()]
            if not years:
                raise ValueError("No valid years provided")
        except ValueError as e:
            if "invalid literal" in str(e):
                raise ValueError("Years must be comma-separated numbers (e.g., 2021,2022,2023)")
            raise e
        
        # Prepare API data exactly matching your Postman structure
        api_data = {
            'province': request.POST.get('province', '').strip(),
            'district': request.POST.get('district', '').strip(),
            'years': years,
            'save_to_postgres': True
        }
        
        # Validate required fields
        if not api_data['province']:
            raise ValueError("Province field is required")
        if not api_data['district']:
            raise ValueError("District field is required")
        
        logger.info(f"Malaria API data: {api_data}")
        
        # Call your working API endpoint
        api_url = f'{base_url}/etl/api/malaria/calculate/'
        response = requests.post(
            api_url,
            json=api_data,
            headers={'Content-Type': 'application/json'},
            timeout=120
        )
        
        logger.info(f"API Response Status: {response.status_code}")
        
        if response.status_code == 200:
            try:
                result = response.json()
                success_msg = f"Malaria API calculation completed successfully! {result.get('message', '')}"
                
                # Return JSON for AJAX, redirect for normal form submission
                if is_ajax:
                    return JsonResponse({
                        'success': True,
                        'message': success_msg,
                        'records_processed': result.get('records_processed', result.get('total_records', 'N/A')),
                        'table_name': result.get('table_name', 'malaria_data')
                    })
                else:
                    messages.success(request, success_msg)
                    return redirect('etl_app:etl_dashboard')
                    
            except json.JSONDecodeError:
                success_msg = "Malaria API calculation completed successfully!"
                if is_ajax:
                    return JsonResponse({'success': True, 'message': success_msg})
                else:
                    messages.success(request, success_msg)
                    return redirect('etl_app:etl_dashboard')
        else:
            error_msg = response.text[:200] if response.text else "No error details"
            logger.error(f"Malaria API failed: {error_msg}")
            
            if is_ajax:
                return JsonResponse({
                    'success': False,
                    'error': f"Malaria API failed with status {response.status_code}: {error_msg}"
                }, status=response.status_code)
            else:
                messages.error(request, f"Malaria API failed with status {response.status_code}: {error_msg}")
                return redirect('etl_app:etl_dashboard')
            
    except requests.Timeout:
        error_msg = "Malaria API request timed out. Please try again."
        if is_ajax:
            return JsonResponse({'success': False, 'error': error_msg}, status=408)
        else:
            messages.error(request, error_msg)
            return redirect('etl_app:etl_dashboard')
            
    except requests.RequestException as e:
        logger.error(f"Malaria API call failed: {str(e)}")
        error_msg = f"Error calling Malaria API: {str(e)}"
        if is_ajax:
            return JsonResponse({'success': False, 'error': error_msg}, status=500)
        else:
            messages.error(request, error_msg)
            return redirect('etl_app:etl_dashboard')
            
    except ValueError as e:
        if is_ajax:
            return JsonResponse({'success': False, 'error': f"Input error: {str(e)}"}, status=400)
        else:
            messages.error(request, f"Input error: {str(e)}")
            return redirect('etl_app:etl_dashboard')
            
    except Exception as e:
        logger.error(f"Malaria processing error: {str(e)}", exc_info=True)
        if is_ajax:
            return JsonResponse({'success': False, 'error': f"Error processing malaria data: {str(e)}"}, status=500)
        else:
            messages.error(request, f"Error processing malaria data: {str(e)}")
            return redirect('etl_app:etl_dashboard')

def process_health_center_etl(request, base_url, is_ajax=False):
    """Process Health Center Lab Data ETL"""
    logger.info("=== HEALTH CENTER ETL PROCESSING ===")
    
    try:
        # Parse years
        years_str = request.POST.get('years', '').strip()
        if not years_str:
            raise ValueError("Years field is required")
        
        try:
            years = [int(year.strip()) for year in years_str.split(',') if year.strip()]
            if not years:
                raise ValueError("No valid years provided")
        except ValueError as e:
            if "invalid literal" in str(e):
                raise ValueError("Years must be comma-separated numbers (e.g., 2021,2022,2023)")
            raise e
        
        # Prepare API data
        api_data = {
            'years': years,
            'district': request.POST.get('district', '').strip(),
            'sector': request.POST.get('sector', '').strip(),
            'save_to_postgres': True,
            'show_available': False
        }
        
        # Validate required fields
        if not api_data['district']:
            raise ValueError("District field is required")
        if not api_data['sector']:
            raise ValueError("Sector field is required")
        
        logger.info(f"Health Center API data: {api_data}")
        
        api_url = f'{base_url}/etl/hc/lab-data/'
        response = requests.post(
            api_url,
            json=api_data,
            headers={'Content-Type': 'application/json'},
            timeout=300
        )
        
        if response.status_code == 200:
            try:
                result = response.json()
                success_msg = f"Health Center data processed successfully! {result.get('message', '')}"
                
                if is_ajax:
                    return JsonResponse({
                        'success': True,
                        'message': success_msg,
                        'records_processed': result.get('records_processed', result.get('total_records', 'N/A')),
                        'table_name': result.get('table_name', 'health_center_data')
                    })
                else:
                    messages.success(request, success_msg)
                    return redirect('etl_app:etl_dashboard')
                    
            except json.JSONDecodeError:
                success_msg = "Health Center data processing completed successfully!"
                if is_ajax:
                    return JsonResponse({'success': True, 'message': success_msg})
                else:
                    messages.success(request, success_msg)
                    return redirect('etl_app:etl_dashboard')
        else:
            error_msg = response.text[:200] if response.text else "No error details"
            if is_ajax:
                return JsonResponse({
                    'success': False,
                    'error': f"Health Center ETL failed with status {response.status_code}: {error_msg}"
                }, status=response.status_code)
            else:
                messages.error(request, f"Health Center ETL failed with status {response.status_code}: {error_msg}")
                return redirect('etl_app:etl_dashboard')
            
    except Exception as e:
        logger.error(f"Health Center processing error: {str(e)}", exc_info=True)
        if is_ajax:
            return JsonResponse({'success': False, 'error': f"Error processing health center data: {str(e)}"}, status=500)
        else:
            messages.error(request, f"Error processing health center data: {str(e)}")
            return redirect('etl_app:etl_dashboard')

def process_weather_etl(request, base_url, is_ajax=False):
    """Process Weather Precipitation & Temperature ETL"""
    logger.info("=== WEATHER ETL PROCESSING ===")
    
    try:
        # Parse years
        years_str = request.POST.get('years', '').strip()
        if not years_str:
            raise ValueError("Years field is required")
        
        try:
            years = [int(year.strip()) for year in years_str.split(',') if year.strip()]
            if not years:
                raise ValueError("No valid years provided")
        except ValueError as e:
            if "invalid literal" in str(e):
                raise ValueError("Years must be comma-separated numbers (e.g., 2021,2022,2023)")
            raise e
        
        # Prepare API data
        api_data = {
            'years': years,
            'save_to_postgres': True,
            'show_available': False
        }
        
        # Add station data if provided
        station_temp = request.POST.get('station_temp', '').strip()
        station_prec = request.POST.get('station_prec', '').strip()
        
        if station_temp:
            api_data['station_temp'] = station_temp
        if station_prec:
            api_data['station_prec'] = station_prec
        
        logger.info(f"Weather API data: {api_data}")
        
        api_url = f'{base_url}/etl/weather/prec-temp/data/'
        response = requests.post(
            api_url,
            json=api_data,
            headers={'Content-Type': 'application/json'},
            timeout=120
        )
        
        if response.status_code == 200:
            try:
                result = response.json()
                success_msg = f"Weather data processed successfully! {result.get('message', '')}"
                
                if is_ajax:
                    return JsonResponse({
                        'success': True,
                        'message': success_msg,
                        'records_processed': result.get('records_processed', result.get('total_records', 'N/A')),
                        'table_name': result.get('table_name', 'weather_data')
                    })
                else:
                    messages.success(request, success_msg)
                    return redirect('etl_app:etl_dashboard')
                    
            except json.JSONDecodeError:
                success_msg = "Weather data processing completed successfully!"
                if is_ajax:
                    return JsonResponse({'success': True, 'message': success_msg})
                else:
                    messages.success(request, success_msg)
                    return redirect('etl_app:etl_dashboard')
        else:
            error_msg = response.text[:200] if response.text else "No error details"
            if is_ajax:
                return JsonResponse({
                    'success': False,
                    'error': f"Weather ETL failed with status {response.status_code}: {error_msg}"
                }, status=response.status_code)
            else:
                messages.error(request, f"Weather ETL failed with status {response.status_code}: {error_msg}")
                return redirect('etl_app:etl_dashboard')
            
    except Exception as e:
        logger.error(f"Weather processing error: {str(e)}", exc_info=True)
        if is_ajax:
            return JsonResponse({'success': False, 'error': f"Error processing weather data: {str(e)}"}, status=500)
        else:
            messages.error(request, f"Error processing weather data: {str(e)}")
            return redirect('etl_app:etl_dashboard')

def process_boundaries_etl(request, base_url, is_ajax=False):
    """Process Administrative Boundaries ETL"""
    logger.info("=== BOUNDARIES ETL PROCESSING ===")
    
    try:
        # Prepare API data
        api_data = {
            'province': request.POST.get('province', '').strip(),
            'district': request.POST.get('district', '').strip(),
            'sector': request.POST.get('sector', '').strip(),
            'transform_coords': True,
            'save_to_postgres': True,
            'update_mode': request.POST.get('update_mode', 'replace')
        }
        
        # Remove empty optional fields
        api_data = {k: v for k, v in api_data.items() if v not in ['', None, False] or k in ['save_to_postgres', 'transform_coords']}
        
        logger.info(f"Boundaries API data: {api_data}")
        
        api_url = f'{base_url}/etl/shapefile/admin-boundaries/'
        response = requests.post(
            api_url,
            json=api_data,
            headers={'Content-Type': 'application/json'},
            timeout=180
        )
        
        if response.status_code == 200:
            try:
                result = response.json()
                success_msg = f"Boundaries ETL completed successfully! {result.get('message', '')}"
                
                if is_ajax:
                    return JsonResponse({
                        'success': True,
                        'message': success_msg,
                        'records_processed': result.get('records_processed', result.get('total_records', 'N/A')),
                        'table_name': result.get('table_name', 'boundaries_data')
                    })
                else:
                    messages.success(request, success_msg)
                    return redirect('etl_app:etl_dashboard')
                    
            except json.JSONDecodeError:
                success_msg = "Boundaries ETL processing completed successfully!"
                if is_ajax:
                    return JsonResponse({'success': True, 'message': success_msg})
                else:
                    messages.success(request, success_msg)
                    return redirect('etl_app:etl_dashboard')
        else:
            error_msg = response.text[:200] if response.text else "No error details"
            if is_ajax:
                return JsonResponse({
                    'success': False,
                    'error': f"Boundaries ETL failed with status {response.status_code}: {error_msg}"
                }, status=response.status_code)
            else:
                messages.error(request, f"Boundaries ETL failed with status {response.status_code}: {error_msg}")
                return redirect('etl_app:etl_dashboard')
            
    except Exception as e:
        logger.error(f"Boundaries processing error: {str(e)}", exc_info=True)
        if is_ajax:
            return JsonResponse({'success': False, 'error': f"Error processing boundaries data: {str(e)}"}, status=500)
        else:
            messages.error(request, f"Error processing boundaries data: {str(e)}")
            return redirect('etl_app:etl_dashboard')

def process_slope_etl(request, base_url, is_ajax=False):
    """Process Slope GeoJSON ETL"""
    logger.info("=== SLOPE ETL PROCESSING ===")
    
    try:
        # Prepare API data
        api_data = {
            'extraction_type': request.POST.get('extraction_type', 'coordinates'),
            'district': request.POST.get('district', '').strip(),
            'sector': request.POST.get('sector', '').strip(),
            'save_to_postgres': True,
            'calculate_statistics': True,
            'update_mode': 'replace'
        }
        
        # Validate required fields
        if not api_data['district']:
            raise ValueError("District field is required")
        if not api_data['sector']:
            raise ValueError("Sector field is required")
        
        # Add coordinates if extraction type is coordinates
        if api_data['extraction_type'] == 'coordinates':
            try:
                coordinates = []
                for coord_field in ['min_lon', 'min_lat', 'max_lon', 'max_lat']:
                    coord_value = request.POST.get(coord_field)
                    if coord_value:
                        coordinates.append(float(coord_value))
                
                if len(coordinates) == 4:
                    api_data['coordinates'] = coordinates
                elif coordinates:
                    raise ValueError("All four coordinate values are required (min_lon, min_lat, max_lon, max_lat)")
            except ValueError as e:
                if "could not convert" in str(e):
                    raise ValueError("Coordinate values must be valid numbers")
                raise e
        
        logger.info(f"Slope API data: {api_data}")
        
        api_url = f'{base_url}/etl/extract/slope-geojson/'
        response = requests.post(
            api_url,
            json=api_data,
            headers={'Content-Type': 'application/json'},
            timeout=180
        )
        
        if response.status_code == 200:
            try:
                result = response.json()
                success_msg = f"Slope data processed successfully! {result.get('message', '')}"
                
                if is_ajax:
                    return JsonResponse({
                        'success': True,
                        'message': success_msg,
                        'records_processed': result.get('records_processed', result.get('total_records', 'N/A')),
                        'table_name': result.get('table_name', 'slope_data')
                    })
                else:
                    messages.success(request, success_msg)
                    return redirect('etl_app:etl_dashboard')
                    
            except json.JSONDecodeError:
                success_msg = "Slope data processing completed successfully!"
                if is_ajax:
                    return JsonResponse({'success': True, 'message': success_msg})
                else:
                    messages.success(request, success_msg)
                    return redirect('etl_app:etl_dashboard')
        else:
            error_msg = response.text[:200] if response.text else "No error details"
            if is_ajax:
                return JsonResponse({
                    'success': False,
                    'error': f"Slope ETL failed with status {response.status_code}: {error_msg}"
                }, status=response.status_code)
            else:
                messages.error(request, f"Slope ETL failed with status {response.status_code}: {error_msg}")
                return redirect('etl_app:etl_dashboard')
            
    except Exception as e:
        logger.error(f"Slope processing error: {str(e)}", exc_info=True)
        if is_ajax:
            return JsonResponse({'success': False, 'error': f"Error processing slope data: {str(e)}"}, status=500)
        else:
            messages.error(request, f"Error processing slope data: {str(e)}")
            return redirect('etl_app:etl_dashboard')

@login_required
def test_etl_apis(request):
    """Test all ETL API endpoints"""
    base_url = request.build_absolute_uri('/').rstrip('/')
    test_results = {}
    
    api_endpoints = [
        f'{base_url}/etl/api/malaria/calculate/',
        f'{base_url}/etl/hc/lab-data/',
        f'{base_url}/etl/weather/prec-temp/data/',
        f'{base_url}/etl/shapefile/admin-boundaries/',
        f'{base_url}/etl/extract/slope-geojson/'
    ]
    
    for api_url in api_endpoints:
        try:
            response = requests.head(api_url, timeout=5)
            test_results[api_url] = {
                'status': response.status_code,
                'accessible': True,
                'message': 'API endpoint is accessible'
            }
        except requests.Timeout:
            test_results[api_url] = {
                'status': 'Timeout',
                'accessible': False,
                'error': 'Request timed out'
            }
        except Exception as e:
            test_results[api_url] = {
                'status': 'Error',
                'accessible': False,
                'error': str(e)
            }
    
    return JsonResponse({
        'status': 'success',
        'api_tests': test_results,
        'tested_at': datetime.now().isoformat()
    })
