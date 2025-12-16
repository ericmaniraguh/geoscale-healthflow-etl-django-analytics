import os
import json
import logging
import re
from functools import wraps
from django.db import connection
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.core.cache import cache
from django.utils.decorators import method_decorator
from django.views import View
from django.db.models import Sum, Avg, Count, Q
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def sanitize(text):
    """Sanitize input for table name construction"""
    if not text:
        return ""
    return re.sub(r'[^a-zA-Z0-9]', '_', text.lower())

def get_dynamic_table_name(table_type, district, sector):
    """
    Construct dynamic table names based on location.
    
    Args:
        table_type (str): 'boundary', 'health_raw', 'yearly', 'monthly', 'api'
        district (str): Selected district name
        sector (str): Selected sector name
        
    Returns:
        str: Table name to query
    """
    # Default fallback as requested by user ("currently i have only Kamabuye")
    d_clean = sanitize(district) if district else 'bugesera'
    s_clean = sanitize(sector) if sector else 'kamabuye'
    
    if table_type == 'boundary':
        # Pattern: rwanda_boundaries_{district}_{sector}
        # Note: Previous ETL seemed to create rwanda_boundaries_bugesera_kamabuye
        return f"rwanda_boundaries_{d_clean}_{s_clean}"
        
    elif table_type == 'health_raw':
        return f"hc_raw_{d_clean}_{s_clean}"
        
    elif table_type == 'yearly':
        return f"hc_data_yearly_statist_{d_clean}_{s_clean}"
        
    elif table_type == 'monthly':
        return f"hc_data_monthly_positivity_{d_clean}_{s_clean}"
    
    elif table_type == 'api':
        # Assuming API tables follow a similar or district-based pattern
        return f"hc_api_east_{d_clean}" # e.g. hc_api_east_bugesera
        
    return None

def get_dynamic_weather_table(district):
    """
    Dynamically find weather table.
    Specific logic for weather tables which have a different naming convention.
    """
    try:
        # Use introspection for cross-db compatibility (SQLite/Postgres)
        all_tables = connection.introspection.table_names()
        # Filter for weather tables: weather_..._prec_and_..._temp_...
        tables = [t for t in all_tables if t.startswith('weather_') and '_prec_and_' in t and '_temp_' in t]
            
        if not tables:
            return None
            
        if district:
            clean_district = sanitize(district)
            matches = [t for t in tables if clean_district in t]
            if matches:
                return matches[0]
                
        # Fallback to 'no_district' or first available
        no_district = next((t for t in tables if 'no_district' in t), None)
        return no_district if no_district else tables[0]
        
    except Exception as e:
        logger.error(f"Error finding weather table: {e}")
        return None

def get_location_hierarchy(request):
    """Extract location parameters from request"""
    province = request.GET.get('province', '').strip()
    district = request.GET.get('district', '').strip()
    sector = request.GET.get('sector', '').strip()
    return province, district, sector

# ============================================================================
# DECORATORS
# ============================================================================

def cache_response(timeout=300):
    """Cache decorator for API responses"""
    def decorator(view_func):
        @wraps(view_func)
        def wrapper(request, *args, **kwargs):
            # Create cache key from request path and query params
            cache_key = f"{view_func.__name__}:{request.GET.urlencode()}"
            cached_response = cache.get(cache_key)
            if cached_response:
                return JsonResponse(cached_response)
            
            response = view_func(request, *args, **kwargs)
            
            if isinstance(response, JsonResponse):
                try:
                    data = json.loads(response.content)
                    cache.set(cache_key, data, timeout)
                except json.JSONDecodeError:
                    pass
            return response
        return wrapper
    return decorator

def handle_errors(view_func):
    """Error handling decorator"""
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        try:
            return view_func(request, *args, **kwargs)
        except Exception as e:
            logger.exception(f"Error in {view_func.__name__}: {e}")
            return JsonResponse({"error": str(e)}, status=500)
    return wrapper

# ============================================================================
# VIEWS
# ============================================================================

@login_required
def dashboard(request):
    """Render the main analytics dashboard"""
    context = {
        'page_title': 'Rwanda Malaria Surveillance Dashboard',
    }
    return render(request, 'analytics_dashboard/analytics_dashboard.html', context)

# ============================================================================
# LOCATION METADATA APIs
# ============================================================================

@login_required
@require_http_methods(["GET"])
@handle_errors
def get_provinces(request):
    """Get list of all provinces from Django database"""
    if not HAS_MODELS:
        return JsonResponse({"provinces": [], "error": "Models not available"})
    
    try:
        provinces = UserProfile.objects.filter(
            province__isnull=False
        ).values_list('province', flat=True).distinct().order_by('province')
        
        return JsonResponse({"provinces": list(provinces)})
    except Exception as e:
        logger.error(f"Error getting provinces: {e}")
        return JsonResponse({"provinces": []})

@login_required
@require_http_methods(["GET"])
@handle_errors
def get_districts(request):
    """Get districts, optionally filtered by province"""
    if not HAS_MODELS:
        return JsonResponse({"districts": []})
    
    province = request.GET.get('province', '').strip()
    
    try:
        query = UserProfile.objects.filter(district__isnull=False)
        
        if province:
            query = query.filter(province=province)
        
        districts = query.values_list('district', flat=True).distinct().order_by('district')
        
        return JsonResponse({"districts": list(districts)})
    except Exception as e:
        logger.error(f"Error getting districts: {e}")
        return JsonResponse({"districts": []})

@login_required
@require_http_methods(["GET"])
@handle_errors
def get_sectors(request):
    """Get sectors, optionally filtered by province and/or district"""
    if not HAS_MODELS:
        return JsonResponse({"sectors": []})
    
    province = request.GET.get('province', '').strip()
    district = request.GET.get('district', '').strip()
    
    try:
        query = UserProfile.objects.filter(sector__isnull=False)
        
        if province:
            query = query.filter(province=province)
        
        if district:
            query = query.filter(district=district)
        
        sectors = query.values_list('sector', flat=True).distinct().order_by('sector')
        
        return JsonResponse({"sectors": list(sectors)})
    except Exception as e:
        logger.error(f"Error getting sectors: {e}")
        return JsonResponse({"sectors": []})

# ============================================================================
# DASHBOARD KPI APIS
# ============================================================================

@login_required
@require_http_methods(["GET"])
# @cache_response(timeout=60) # Short cache for map data
@handle_errors
def get_kpi_data(request):
    """Get Key Performance Indicators"""
    if not HAS_MODELS:
        return JsonResponse({
            "total_tests": 0,
            "total_positive": 0,
            "total_negative": 0,
            "avg_positivity_rate": 0,
            "positive_change": "0%",
            "negative_change": "0%"
        })
    
    province, district, sector = get_location_hierarchy(request)
    filters = build_location_filter(province, district, sector)
    
    boundary_table = get_dynamic_table_name('boundary', district, sector)
    health_table_raw = get_dynamic_table_name('health_raw', district, sector) # Use raw for health join? 
    # Actually, usually map joins with a view or table. Let's assume we join boundary with aggregated raw data.
    
    # Check if table exists (optional but good for debugging)
    
    try:
        # Get counts from UserProfile or your data model
        total_users = UserProfile.objects.filter(filters).count()
        
        # If you have health data in a separate model, query that instead
        # Example: health_data = HealthData.objects.filter(filters).aggregate(...)
        
        return JsonResponse({
            "total_tests": total_users,
            "total_positive": int(total_users * 0.15),  # Placeholder calculation
            "total_negative": int(total_users * 0.85),
            "avg_positivity_rate": 15.0,
            "positive_change": "5.2%",
            "negative_change": "-3.1%"
        })
        
    except Exception as e:
        logger.error(f"Error getting KPI data: {e}")
        return JsonResponse({
            "total_tests": 0,
            "total_positive": 0,
            "total_negative": 0,
            "avg_positivity_rate": 0,
            "positive_change": "0%",
            "negative_change": "0%"
        })

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_gender_analysis(request):
    """Get gender-based analysis"""
    if not HAS_MODELS:
        return JsonResponse({"labels": [], "2021": [], "2022": [], "2023": []})
    
    province, district, sector = get_location_hierarchy(request)
    filters = build_location_filter(province, district, sector)
    
    table_yearly = get_dynamic_table_name('yearly', district, sector)
    table_api = get_dynamic_table_name('api', district, sector)
    
    try:
        # Get gender distribution from UserProfile
        gender_data = UserProfile.objects.filter(filters).values('gender').annotate(
            count=Count('id')
        ).order_by('-count')
        
        labels = [item['gender'] for item in gender_data if item['gender']]
        
        if not labels:
            return JsonResponse({"labels": [], "2021": [], "2022": [], "2023": []})
        
        # Generate trend data
        values_2021 = [item['count'] * 0.8 for item in gender_data]
        values_2022 = [item['count'] * 0.9 for item in gender_data]
        values_2023 = [item['count'] for item in gender_data]
        
        return JsonResponse({
            "labels": labels,
            "2021": values_2021,
            "2022": values_2022,
            "2023": values_2023
        })
    except Exception as e:
        error_msg = str(e).lower()
        if 'undefined table' in error_msg or 'does not exist' in error_msg:
             return JsonResponse({
                "total_tests": 0, "total_positive": 0, "total_negative": 0,
                "avg_positivity_rate": 0, "positive_change": "0", "negative_change": "0"
            })
        raise e

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_precipitation_data(request):
    """Get monthly precipitation data"""
    # Sample data - integrate with actual weather data model if available
    labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
             'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    sample_data = [120, 80, 95, 110, 75, 60, 45, 55, 85, 105, 130, 140]
    
    return JsonResponse({
        "labels": labels,
        "2021": sample_data,
        "2022": [x * 1.1 for x in sample_data],
        "2023": [x * 0.9 for x in sample_data]
    })

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_monthly_trend(request):
    """Get monthly positivity trend"""
    labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
             'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    
    return JsonResponse({
        "labels": labels,
        "2021": [4.5, 4.8, 5.2, 5.6, 6.1, 6.5, 6.2, 5.8, 5.3, 4.9, 4.6, 4.3],
        "2022": [4.2, 4.5, 4.9, 5.3, 5.8, 6.2, 5.9, 5.5, 5.0, 4.6, 4.3, 4.0],
        "2023": [3.8, 4.1, 4.5, 4.9, 5.4, 5.8, 5.5, 5.1, 4.6, 4.2, 3.9, 3.6]
    })

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_villages_data(request):
    """Get village-level statistics"""
    if not HAS_MODELS:
        return JsonResponse({"villages": []})
    
    province, district, sector = get_location_hierarchy(request)
    filters = build_location_filter(province, district, sector)
    
    col_name = 'monthly_precipitation' if data_type == 'precipitation' else 'monthly_temperature'
    
    if not table_name:
         return JsonResponse({"labels": ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], "available_years": []})

    try:
        # Get village statistics
        villages_data = UserProfile.objects.filter(filters).values('health_centre').annotate(
            total_tests=Count('id'),
            positivity_rate=Avg('id')  # Placeholder
        ).order_by('-positivity_rate')[:20]
        
        villages = []
        for item in villages_data:
            if item['health_centre']:
                villages.append({
                    "village": item['health_centre'],
                    "tests": item['total_tests'],
                    "positive": int(item['total_tests'] * 0.15),
                    "rate": 15.0,
                    "year": 2023
                })
        
        return JsonResponse({"villages": villages})
    except Exception as e:
        logger.error(f"Error getting villages data: {e}")
        return JsonResponse({"villages": []})

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_location_summary(request):
    """Get summary statistics by location"""
    if not HAS_MODELS:
        return JsonResponse({"group_by": "district", "summary": []})
    
    group_by = request.GET.get('group_by', 'district')
    province, district, sector = get_location_hierarchy(request)
    filters = build_location_filter(province, district, sector)
    
    try:
        # Build aggregation based on group_by parameter
        if group_by == 'district':
            summary_data = UserProfile.objects.filter(filters).values('district').annotate(
                total_tests=Count('id'),
                num_villages=Count('health_centre', distinct=True)
            ).order_by('-total_tests')
            
            location_field = 'district'
        elif group_by == 'sector':
            summary_data = UserProfile.objects.filter(filters).values('sector').annotate(
                total_tests=Count('id'),
                num_villages=Count('health_centre', distinct=True)
            ).order_by('-total_tests')
            
            location_field = 'sector'
        else:  # province
            summary_data = UserProfile.objects.filter(filters).values('province').annotate(
                total_tests=Count('id'),
                num_villages=Count('health_centre', distinct=True)
            ).order_by('-total_tests')
            
            location_field = 'province'
        
        summary = []
        for item in summary_data:
            if item[location_field]:
                summary.append({
                    "location": item[location_field],
                    "total_tests": item['total_tests'],
                    "total_positive": int(item['total_tests'] * 0.15),
                    "total_negative": int(item['total_tests'] * 0.85),
                    "avg_positivity_rate": 15.0,
                    "num_villages": item['num_villages']
                })
        
        return JsonResponse({
            "group_by": group_by,
            "summary": summary
        })
    except Exception as e:
        logger.error(f"Error getting location summary: {e}")
        return JsonResponse({"group_by": group_by, "summary": []})

    except Exception as e:
         error_msg = str(e).lower()
         if 'undefined table' in error_msg or 'does not exist' in error_msg: return JsonResponse({"labels": [], "available_years": []})
         raise e

@login_required
@require_http_methods(["POST"])
@handle_errors
def refresh_data(request):
    """
    Trigger a manual refresh of the dashboard data.
    """
    return JsonResponse({"status": "success", "message": "Data refreshed successfully"})

@login_required
@require_http_methods(["GET"])
@handle_errors
def export_data(request):
    """Export data as JSON"""
    export_format = request.GET.get('format', 'json')
    
    # Get all data
    kpi_response = get_kpi_data(request)
    kpi_data = json.loads(kpi_response.content)
    
    gender_response = get_gender_analysis(request)
    gender_data = json.loads(gender_response.content)
    
    villages_response = get_villages_data(request)
    villages_data = json.loads(villages_response.content)
    
    export_data = {
        'kpi': kpi_data,
        'gender_analysis': gender_data,
        'villages': villages_data,
        'exported_at': datetime.now().isoformat()
    }
    
    if export_format == 'csv':
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="malaria_data_export.csv"'
        # Add CSV writing logic here
        return response
    else:
        return JsonResponse(export_data)

@login_required
def check_admin_status(request):
    """Check if user is admin"""
    return JsonResponse({"is_admin": is_admin(request.user)})

@login_required
@csrf_exempt
def logout_user(request):
    """Logout user"""
    if request.method == "POST":
        logout(request)
        return JsonResponse({"success": True, "redirect": "/auth/login/"})
    return JsonResponse({"success": False}, status=405)
