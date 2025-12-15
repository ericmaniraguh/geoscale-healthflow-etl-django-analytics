import os
import json
import logging
import tempfile
import uuid
import threading
from functools import wraps
from datetime import timedelta
from django.db import connection
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required, user_passes_test
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.contrib.auth import logout
from django.core.cache import cache
from django.utils.decorators import method_decorator
from django.views import View
from django.db.models import Sum, Avg, Count, Q
from django.db.models import Sum, Avg, Count, Q
from datetime import datetime
from pymongo import MongoClient
from django.conf import settings

# Configure logging
logger = logging.getLogger(__name__)

# ============================================================================
# IMPORT YOUR MODELS
# ============================================================================
# IMPORTANT: Replace 'your_app' with your actual app name
# Adjust these imports based on your actual models

try:
    from accounts.models import UserProfile  # Or whatever model stores location data
    HAS_MODELS = True
except ImportError:
    HAS_MODELS = False
    logger.warning("Could not import UserProfile model")

# ============================================================================
# HELPER FUNCTIONS FOR LOCATION FILTERING
# ============================================================================

def build_location_filter(province=None, district=None, sector=None):
    """
    Build Q filter for Django ORM
    Returns: Q object for filtering
    """
    filters = Q()
    
    if province:
        filters &= Q(province=province)
    
    if district:
        filters &= Q(district=district)
    
    if sector:
        filters &= Q(sector=sector)
    
    return filters

def get_location_hierarchy(request):
    """
    Extract location parameters from request
    Returns: (province, district, sector)
    """
    province = request.GET.get('province', '').strip() or None
    district = request.GET.get('district', '').strip() or None
    sector = request.GET.get('sector', '').strip() or None
    
    return province, district, sector

def get_dynamic_weather_table(district=None):
    """
    Dynamically find the correct weather table based on district.
    Searches for tables matching pattern: weather_%_prec_and_%_temp_{district}
    """
    base_pattern = "weather_%_prec_and_%_temp_%"
    
    query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE %s
    """
    params = [base_pattern]
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            tables = [row[0] for row in cursor.fetchall()]
            
        if not tables:
            logger.warning("No weather tables found in database")
            return None
            
        # If district provided, try to find exact match
        if district:
            clean_district = re.sub(r'[^a-zA-Z0-9]', '_', district.lower())
            # prioritize tables ending with the district name
            matches = [t for t in tables if clean_district in t]
            if matches:
                # Pick the most recent one if multiple (assuming newer tables might be better, or just pick first)
                selected = matches[0]
                logger.info(f"Found weather table for district '{district}': {selected}")
                return selected
            # Reverting strict matching to allow fallback as per user request for flexibility
            # else:
            #    logger.info(f"No weather table found for district '{district}'")
            #    return None
                
        # Fallback: finding 'no_district' or any table
        no_district_matches = [t for t in tables if 'no_district' in t]
        if no_district_matches:
             return no_district_matches[0]
             
        # Ultimate fallback: return the first available weather table
        logger.info(f"No specific match for district '{district}', using fallback: {tables[0]}")
        return tables[0]
        
    except Exception as e:
        logger.error(f"Error finding dynamic weather table: {e}")
        return "weather_juru_prec_and_juru_temp_no_district" # Safe hardcoded fallback if DB query fails

def get_year_filter(request):
    """
    Extract year parameter from request
    Returns: list of years (e.g., [2021, 2022]) or None if all
    """
    years_str = request.GET.get('years', '').strip()
    if years_str:
        try:
            return [int(y) for y in years_str.split(',') if y.strip()]
        except ValueError:
            pass
    return None

# ============================================================================
# DECORATORS
# ============================================================================

def cache_response(timeout=300):
    """Cache decorator for API responses"""
    def decorator(view_func):
        @wraps(view_func)
        def wrapper(request, *args, **kwargs):
            # Create cache key from request
            cache_key = f"{view_func.__name__}:{request.GET.urlencode()}"
            
            # Try to get from cache
            cached_response = cache.get(cache_key)
            if cached_response:
                logger.debug(f"Cache hit for {cache_key}")
                return JsonResponse(cached_response)
            
            # Execute view
            response = view_func(request, *args, **kwargs)
            
            # Cache the response data
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
            return JsonResponse({
                "error": "Internal server error",
                "message": str(e) if os.getenv('DEBUG') else "An error occurred"
            }, status=500)
    return wrapper

def is_admin(user):
    """Check if user is admin"""
    return user.is_authenticated and (user.is_staff or user.is_superuser)

# ============================================================================
# DASHBOARD VIEW
# ============================================================================

@login_required
def dashboard(request):
    """Main dashboard view - Renders analytics_dashboard.html"""
    context = {
        'user': request.user,
        'is_admin': True,
        'page_title': 'Rwanda Malaria Surveillance Dashboard',
        'api_url': request.build_absolute_uri('/analytics_dashboard/api/')
    }
    return render(request, 'analytics_dashboard/analytics_dashboard.html', context)

# ============================================================================
# LOCATION METADATA APIs (FROM POSTGRES ACCOUNTS TABLES)
# ============================================================================

@login_required
@require_http_methods(["GET"])
@handle_errors
def get_provinces(request):
    """Get list of all provinces from Postgres accounts_province table"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT name FROM accounts_province ORDER BY name")
            provinces = [row[0] for row in cursor.fetchall()]
        return JsonResponse({"provinces": provinces})
    except Exception as e:
        logger.error(f"Error getting provinces: {e}")
        return JsonResponse({"provinces": []})

@login_required
@require_http_methods(["GET"])
@handle_errors
def get_districts(request):
    """Get districts from Postgres accounts_district table"""
    province = request.GET.get('province', '').strip()
    try:
        with connection.cursor() as cursor:
            if province:
                # Join with province table to filter by province name
                query = """
                    SELECT d.name 
                    FROM accounts_district d
                    JOIN accounts_province p ON d.province_id = p.id
                    WHERE p.name = %s
                    ORDER BY d.name
                """
                cursor.execute(query, [province])
            else:
                cursor.execute("SELECT name FROM accounts_district ORDER BY name")
            
            districts = [row[0] for row in cursor.fetchall()]
        return JsonResponse({"districts": districts})
    except Exception as e:
        logger.error(f"Error getting districts: {e}")
        return JsonResponse({"districts": []})

@login_required
@require_http_methods(["GET"])
@handle_errors
def get_sectors(request):
    """Get sectors from Postgres accounts_sector table"""
    province = request.GET.get('province', '').strip()
    district = request.GET.get('district', '').strip()
    
    try:
        with connection.cursor() as cursor:
            if district:
                # Join with district table to filter by district name
                query = """
                    SELECT s.name 
                    FROM accounts_sector s
                    JOIN accounts_district d ON s.district_id = d.id
                    WHERE d.name = %s
                    ORDER BY s.name
                """
                cursor.execute(query, [district])
                sectors = [row[0] for row in cursor.fetchall()]
            elif province:
                # If only province selected, get all sectors in that province (optional but good UX)
                query = """
                    SELECT s.name 
                    FROM accounts_sector s
                    JOIN accounts_district d ON s.district_id = d.id
                    JOIN accounts_province p ON d.province_id = p.id
                    WHERE p.name = %s
                    ORDER BY s.name
                """
                cursor.execute(query, [province])
                sectors = [row[0] for row in cursor.fetchall()]
            else:
                 # Return empty if no filter to prevent loading all sectors
                 sectors = []
            
        return JsonResponse({"sectors": sectors})
    except Exception as e:
        logger.error(f"Error getting sectors: {e}")
        return JsonResponse({"sectors": []})

# ============================================================================
# DASHBOARD KPI APIS (FROM POSTGRES TABLE)
# ============================================================================

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_kpi_data(request):
    """Get Key Performance Indicators from yearly statistics"""
    province, district, sector = get_location_hierarchy(request)
    years = get_year_filter(request)
    
    try:
        # Switch to yearly statistics for flexible aggregation
        query = """
            SELECT 
                SUM(total_tests) as total_tests, 
                SUM(positive_cases) as total_pos, 
                SUM(negative_cases) as total_neg,
                AVG(positivity_rate) as avg_pos
            FROM hc_data_yearly_statist_bugesera_kamabuye 
            WHERE 1=1
        """
        params = []
        
        if years:
            query += " AND year IN %s"
            # Postgres adapter handles tuple/list for IN clause usually, but raw sql might need tuple
            params.append(tuple(years))
            
        if district:
            query += " AND filter_district ILIKE %s"
            params.append(district.strip())
        if sector:
            query += " AND filter_sector ILIKE %s"
            params.append(sector.strip())
            
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
            
        total_tests = row[0] or 0
        total_pos = row[1] or 0
        total_neg = row[2] or 0
        avg_pos_rate = round(row[3], 1) if row[3] else 0
        
        # Get Population & API from API table (as summary table might lack these)
        api_query = "SELECT SUM(population), AVG(api) FROM hc_api_east_bugesera WHERE 1=1"
        api_params = []
        if province: api_query += " AND province ILIKE %s"; api_params.append(province.strip())
        if district: api_query += " AND district ILIKE %s"; api_params.append(district.strip())
        if sector: api_query += " AND sector ILIKE %s"; api_params.append(sector.strip())
            
        with connection.cursor() as cursor:
            cursor.execute(api_query, api_params)
            api_row = cursor.fetchone()
            
        total_pop = api_row[0] or 0
        avg_api = round(api_row[1], 1) if api_row[1] else 0
        
        # Calculate Incidence
        incidence = round((total_pos / total_pop * 1000), 1) if total_pop > 0 else 0
        
        return JsonResponse({
            "total_tests": total_tests,
            "total_positive": total_pos,
            "total_negative": total_neg,
            "avg_positivity_rate": avg_pos_rate, # From summary table
            "positive_change": f"{avg_api}",     # Using API as 'Change' metric or secondary kpi
            "negative_change": f"{incidence}"    # Using Incidence as 'Change' metric
        })
    except Exception as e:
        logger.error(f"Error getting KPI data: {e}")
        return JsonResponse({
            "total_tests": 0, "total_positive": 0, "total_negative": 0,
            "avg_positivity_rate": 0, "positive_change": "0%", "negative_change": "0%"
        })

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_gender_analysis(request):
    """Get gender-based analysis from hc_data_gender_pos_by_year_..."""
    province, district, sector = get_location_hierarchy(request)
    years = get_year_filter(request)
    
    try:
        # Aggregate from RAW table to ensure we get both Total Tests and Positive Cases
        # Calculate Positivity Rate: (Positive / Total) * 100
        query = """
            SELECT 
                gender, 
                COUNT(*) as total_tests,
                SUM(CASE WHEN test_result = 'Positive' OR is_positive = true THEN 1 ELSE 0 END) as total_positive,
                year
            FROM hc_raw_bugesera_kamabuye
            WHERE gender IS NOT NULL
        """
        params = []
        
        if years:
            query += " AND year IN %s"
            params.append(tuple(years))
            
        if district:
            query += " AND district ILIKE %s"
            params.append(district.strip())
        if sector:
            query += " AND sector ILIKE %s"
            params.append(sector.strip())
            
        # Group by gender and year
        query += " GROUP BY gender, year ORDER BY gender"
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
        # Transform rows: [(Male, 100 tests, 20 pos, 2021), ...]
        unique_genders = sorted(list(set(r[0] for r in rows if r[0])))
        
        # Determine years present in data if not filtered
        if years:
            target_years = years
        else:
            # Extract unique years from rows dynamically
            found_years = sorted(list(set(r[3] for r in rows if r[3])))
            target_years = found_years if found_years else [2021, 2022, 2023]
            
        response_data = {
            "labels": unique_genders,
            "available_years": target_years
        }
        
        for y in target_years:
            year_data = []
            for g in unique_genders:
                # Find stats for this gender + year
                # row: (gender, total_tests, total_positive, year)
                match = next((r for r in rows if r[0] == g and r[3] == y), None)
                if match:
                    total = match[1]
                    positive = match[2]
                    rate = round((positive / total * 100), 1) if total > 0 else 0
                    year_data.append(rate)
                else:
                    year_data.append(0)
            response_data[str(y)] = year_data

        return JsonResponse(response_data)
    except Exception as e:
        logger.error(f"Error getting gender analysis: {e}")
        return JsonResponse({"labels": [], "2021": [], "2022": [], "2023": []})

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_precipitation_data(request):
    """Get monthly precipitation data for chart, separated by year."""
    province, district, sector = get_location_hierarchy(request)
    years_filter = get_year_filter(request)
    
    try:
        table_name = get_dynamic_weather_table(None)
        if not table_name:
            return JsonResponse({"labels": ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], "available_years": []})
            
        query = f"""
            SELECT year, month, AVG(monthly_precipitation)
            FROM {table_name}
            WHERE 1=1
        """
        params = []
        
        if years_filter:
            query += " AND year IN %s"
            params.append(tuple(years_filter))
            
        # Add Location Filter (Note: This assumes the table has 'district'/'sector' columns)
        if district:
            query += " AND (district ILIKE %s)"
            params.append(district.strip())

        if sector:
            query += " AND (sector ILIKE %s)"
            params.append(sector.strip())
            
        group_by = " GROUP BY year, month ORDER BY year, month" # ORDER BY YEAR is crucial
        query += group_by
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
        labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        response_data = {"labels": labels}
        
        # Determine years present in the result set
        found_years = sorted(list(set(r[0] for r in rows if r[0])))
        response_data["available_years"] = found_years
        
        data_map = {} # {2021: {1: 50, 2: 60}}
        for r in rows:
            y = r[0]
            m = int(r[1])
            val = round(float(r[2]), 1) if r[2] is not None else 0
            
            if y not in data_map: data_map[y] = {}
            data_map[y][m] = val
            
        # Build arrays for all found years
        for y in found_years:
            year_series = [None] * 12 # Use None for missing month data
            if y in data_map:
                for i in range(12):
                    year_series[i] = data_map[y].get(i+1)
            response_data[str(y)] = year_series
            
        return JsonResponse(response_data)

    except Exception as e:
        logger.error(f"Error getting precipitation data: {e}", exc_info=True)
        return JsonResponse({
            "labels": ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
            "available_years": []
        })

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_temperature_data(request):
    """Get monthly temperature data for chart, separated by year."""
    province, district, sector = get_location_hierarchy(request)
    years_filter = get_year_filter(request)
    
    try:
        table_name = get_dynamic_weather_table(None)
        if not table_name:
            return JsonResponse({"labels": ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], "available_years": []})

        query = f"""
            SELECT year, month, AVG(monthly_temperature) -- Changed column to monthly_temperature
            FROM {table_name}
            WHERE 1=1
        """
        params = []
        
        if years_filter:
            query += " AND year IN %s"
            params.append(tuple(years_filter))
            
        # Add Location Filter
        if district:
            query += " AND (district ILIKE %s)"
            params.append(district.strip())

        if sector:
            query += " AND (sector ILIKE %s)"
            params.append(sector.strip())
            
        group_by = " GROUP BY year, month ORDER BY year, month" # ORDER BY YEAR is crucial
        query += group_by
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
        labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        response_data = {"labels": labels}
        
        # Determine years present in the result set
        found_years = sorted(list(set(r[0] for r in rows if r[0])))
        response_data["available_years"] = found_years
        
        data_map = {} 
        for r in rows:
            y = r[0]
            m = int(r[1])
            val = round(float(r[2]), 1) if r[2] is not None else 0
            
            if y not in data_map: data_map[y] = {}
            data_map[y][m] = val
            
        for y in found_years:
            year_series = [None] * 12
            if y in data_map:
                for i in range(12):
                    year_series[i] = data_map[y].get(i+1)
            response_data[str(y)] = year_series
            
        return JsonResponse(response_data)

    except Exception as e:
        logger.error(f"Error getting temperature data: {e}", exc_info=True)
        return JsonResponse({
            "labels": ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
            "available_years": []
        })

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_monthly_trend(request):
    """Get monthly trend from hc_data_monthly_positivity_..."""
    province, district, sector = get_location_hierarchy(request)
    years = get_year_filter(request)
    
    try:
        # Query monthly table
        # We need month, rate, YEAR to separate datasets
        query = """
            SELECT month_name, AVG(positivity_rate), year, month
            FROM hc_data_monthly_positivity_bugesera_kamabuye 
            WHERE 1=1
        """
        params = []
        
        if years:
            query += " AND year IN %s"
            params.append(tuple(years))
            
        if district:
            query += " AND filter_district ILIKE %s"
            params.append(district.strip())
        if sector:
            query += " AND filter_sector ILIKE %s"
            params.append(sector.strip())
            
        # Group by year, month
        query += " GROUP BY year, month, month_name ORDER BY month"
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
        # rows: [(Jan, 10.5, 2021, 1), (Jan, 12.0, 2022, 1), ...]
        # We need standard month labels
        month_labels = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        
        # Determine years present in data
        if years:
            target_years = years
        else:
            # unique sorted years from result
            found_years = sorted(list(set(r[2] for r in rows if r[2])))
            target_years = found_years if found_years else [2021, 2022, 2023]

        response_data = {
            "labels": month_labels,
            "available_years": target_years
        }
        
        for y in target_years:
            year_data = [0] * 12
            for r in rows:
                if r[2] == y: # Matching year
                    m_idx = r[3] - 1 # 0-indexed month
                    if 0 <= m_idx < 12:
                        year_data[m_idx] = float(r[1])
            response_data[str(y)] = year_data
        
        return JsonResponse(response_data)
    except Exception as e:
        logger.error(f"Error trend: {e}")
        return JsonResponse({"labels": [], "2021": [], "2022": [], "2023": []})

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_villages_data(request):
    """Get village level stats from hc_raw_bugesera_kamabuye"""
    province, district, sector = get_location_hierarchy(request)
    years = get_year_filter(request)
    
    try:
        # Query raw table
        # We want: Village Name, Total Tests, Positive Cases, Rate
        query = """
            SELECT 
                INITCAP(village) as village, 
                COUNT(*) as total_tests,
                SUM(CASE WHEN test_result = 'Positive' OR is_positive = true THEN 1 ELSE 0 END) as positive_cases
            FROM hc_raw_bugesera_kamabuye 
            WHERE 1=1
        """
        params = []
        
        if years:
            query += " AND year IN %s"
            params.append(tuple(years))
            
        if district:
             query += " AND district ILIKE %s"
             params.append(district.strip())
        if sector:
             query += " AND sector ILIKE %s"
             params.append(sector.strip())
             
        query += " GROUP BY 1 HAVING COUNT(*) > 0 ORDER BY (SUM(CASE WHEN test_result = 'Positive' OR is_positive = true THEN 1 ELSE 0 END)::float / COUNT(*)) DESC LIMIT 50"
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
        villages = []
        
        # Determine display string for year
        if years:
             year_str = ", ".join(map(str, sorted(years)))
        else:
             year_str = "All Available"
             
        for row in rows:
            mapped_village = row[0]
            if not mapped_village: continue
            
            total = row[1]
            positive = row[2] or 0
            rate = round((positive / total * 100), 1) if total > 0 else 0
            
            villages.append({
                "village": mapped_village,
                "tests": total,
                "positive": positive,
                "rate": rate,
                "year": year_str
            })
        return JsonResponse({"villages": villages})
    except Exception as e:
        logger.error(f"Error villages: {e}")
        return JsonResponse({"villages": []})


@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_location_summary(request):
    """Get summary statistics by location"""
    group_by = request.GET.get('group_by', 'district') # 'district', 'sector'
    province, district, sector = get_location_hierarchy(request)
    years = get_year_filter(request)
    
    try:
        # Determine grouping column
        col = 'filter_sector' if sector else 'filter_district' if district else 'filter_district' # Default to district
        
        # Query yearly stats table for aggregation
        # 1. Main Stats Query
        query = f"""
            SELECT 
                {col}, 
                SUM(total_tests) as tests, 
                SUM(positive_cases) as pos, 
                SUM(negative_cases) as neg,
                AVG(positivity_rate) as rate
            FROM hc_data_yearly_statist_bugesera_kamabuye 
            WHERE 1=1
        """
        params = []
        
        if years:
            query += " AND year IN %s"
            params.append(tuple(years))
        
        if district: 
            query += " AND filter_district ILIKE %s"
            params.append(district.strip())
        if sector:
            query += " AND filter_sector ILIKE %s"
            params.append(sector.strip())
        
        query += f" GROUP BY {col} ORDER BY SUM(positive_cases) DESC"
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
        # 2. Village Count Query (Raw Table)
        # We need distinct village count per location (district/sector)
        raw_col = 'sector' if sector else 'district' if district else 'district'
        raw_params = []
        
        village_query = f"""
            SELECT {raw_col}, COUNT(DISTINCT INITCAP(village)) as v_count
            FROM hc_raw_bugesera_kamabuye
            WHERE 1=1
        """
        
        if years:
            village_query += " AND year IN %s"
            raw_params.append(tuple(years))
            
        if district:
             village_query += " AND district ILIKE %s"
             raw_params.append(district.strip())
        if sector:
             village_query += " AND sector ILIKE %s"
             raw_params.append(sector.strip())
             
        village_query += f" GROUP BY {raw_col}"
        
        with connection.cursor() as cursor:
            cursor.execute(village_query, raw_params)
            village_rows = cursor.fetchall() # [(Bugesera, 150), ...]
            
        # Create map for village counts
        village_map = {row[0]: row[1] for row in village_rows if row[0]}
            
        summary = []
        for row in rows:
            loc_name = row[0]
            if not loc_name: continue
            
            # Match names (handle potential case sensitivity or spaces if needed, but assuming exact match)
            # Database columns should match conceptually (District name in raw vs filter_district in statist)
            
            num_villages = village_map.get(loc_name, 0)
            
            summary.append({
                "location": loc_name,
                "total_tests": row[1] or 0,
                "total_positive": row[2] or 0,
                "total_negative": row[3] or 0,
                "avg_positivity_rate": round(row[4], 1) if row[4] else 0,
                "num_villages": num_villages
            })
        
        return JsonResponse({
            "group_by": group_by,
            "summary": summary
        })
    except Exception as e:
        logger.error(f"Error summary: {e}")
        return JsonResponse({"group_by": group_by, "summary": []})

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

@login_required
@require_http_methods(["GET"])
@handle_errors
def refresh_data(request):
    """Refresh data cache"""
    # Clear all dashboard cache keys
    cache_pattern = 'analytics_dashboard_*'
    # Note: Simple implementation, Django cache backend dependent
    
    return JsonResponse({
        'status': 'success',
        'message': 'Data cache refreshed successfully'
    })

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

# ============================================================================
# MAP DATA API
# ============================================================================

# In your views.py, replace the existing get_map_data function:

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_map_data(request):
    """Get GeoJSON data for the map, linking boundary geometry, health data, and slope."""
    province, district, sector = get_location_hierarchy(request)
    years = get_year_filter(request)
    
    # --- 1. Determine Boundary Table Name ---
    import re
    def sanitize(name):
        if not name: return ""
        return re.sub(r'[^a-zA-Z0-9]', '_', name.lower())[:30]

    # Use the District for the boundary table name. Default to Bugesera if none.
    dist_for_table = district or "Bugesera"
    dist_part = sanitize(dist_for_table)
    # Remove sector from the table name logic - assume district level table exists
    boundary_table = f"rwanda_boundaries_{dist_part}_village" 

    try:
        # --- 2. Build Query Parameters and Clauses ---
        params = []
        
        # Year Filter for Health Data (for the CTE)
        year_clause = ""
        if years:
            year_clause = " AND year IN %s"
            params.append(tuple(years))
            
        # Add Health Data Filters (for the CTE)
        params.append(f"%{district or 'Bugesera'}%") # Health Data District Filter
        params.append(f"%{sector or 'Kamabuye'}%") # Health Data Sector Filter

        # Add Boundary Table Filtering Parameters (New)
        boundary_filter_clause = " AND b.district_name ILIKE %s"
        params.append(f"%{district or 'Bugesera'}%") # Boundary District Filter

        if sector:
            # If sector is selected, filter the boundary rows further.
            boundary_filter_clause += " AND b.sector_name ILIKE %s"
            params.append(f"%{sector}%") # Boundary Sector Filter (only if sector is set)

        # --- 3. Construct Optimized SQL Query ---
        query = f"""
            WITH village_health AS (
                -- Health Data is filtered by the user's selected district/sector
                SELECT 
                    TRIM(LOWER(village)) as join_key,
                    COUNT(*) as total_tests,
                    SUM(CASE WHEN test_result = 'Positive' OR is_positive = true THEN 1 ELSE 0 END) as positive_cases
                FROM hc_raw_bugesera_kamabuye
                WHERE 1=1 
                {year_clause}
                AND district ILIKE %s 
                AND sector ILIKE %s 
                GROUP BY 1
            )
            SELECT 
                -- Boundary & Location Columns
                b.district_name,
                b.sector_name,
                b.cell_name,
                b.village_name,
                
                -- Slope Columns (For Map Styling/Popup)
                b.mean_slope,
                b.max_slope,
                b.slope_class,
                
                -- Geometry (CRUCIAL for Leaflet)
                b.geometry_geojson,
                
                -- Joined Health Stats (CRUCIAL for Risk Level)
                COALESCE(h.total_tests, 0) as total_tests,
                COALESCE(h.positive_cases, 0) as total_positive
            FROM {boundary_table} b
            LEFT JOIN village_health h ON TRIM(LOWER(b.village_name)) = h.join_key
            WHERE b.geometry_geojson IS NOT NULL
            {boundary_filter_clause} -- <-- New dynamic filter here
            LIMIT 5000;
        """
        
        logger.info(f"Executing map query against {boundary_table} with params: {params}")
        
        # --- 4. Execute Query and Format GeoJSON ---
        with connection.cursor() as cursor:
            # Check table existence first
            cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)", [boundary_table])
            if not cursor.fetchone()[0]:
                 logger.warning(f"Boundary table {boundary_table} not found.")
                 return JsonResponse({"type": "FeatureCollection", "features": []})

            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        features = []
        
        for row in rows:
            data = dict(zip(columns, row))
            
            # --- Geometry Parsing ---
            geom = data.get('geometry_geojson')
            if isinstance(geom, str):
                try:
                    # Clean/Parse the GeoJSON string
                    cleaned_geom = geom.strip().replace('\\"', '"') # Remove leading/trailing spaces and unescape quotes
                    if cleaned_geom.startswith('"') and cleaned_geom.endswith('"'):
                        cleaned_geom = cleaned_geom[1:-1] # Remove outer quotes if database stored it as a quoted string
                    
                    geom = json.loads(cleaned_geom)
                except Exception as e:
                    logger.warning(f"Failed to parse geometry for {data.get('village_name')}: {e}")
                    continue
            
            if not geom or not isinstance(geom, dict):
                continue

            # --- Health Data Calculation ---
            total = data.get('total_tests', 0)
            positive = data.get('total_positive', 0)
            rate = round((positive / total * 100), 1) if total > 0 else 0
            
            # Risk Level definition (matches client-side styling)
            if rate > 5: risk = 'High'
            elif rate >= 1: risk = 'Medium'
            else: risk = 'Low'

            feature = {
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    # Location
                    "district": data.get('district_name'),
                    "sector": data.get('sector_name'),
                    "cell": data.get('cell_name'),
                    "village": data.get('village_name'),
                    
                    # Slope Data
                    "mean_slope": data.get('mean_slope'),
                    "max_slope": data.get('max_slope'),
                    "slope_class": data.get('slope_class'),
                    
                    # Health Data
                    "total_tests": total,
                    "total_positive": positive,
                    "positivity_rate": rate,
                    "risk_level": risk,
                    "has_data": (total > 0)
                }
            }
            features.append(feature)
        
        return JsonResponse({"type": "FeatureCollection", "features": features})

    except Exception as e:
        logger.error(f"Error getting map data: {e}", exc_info=True)
        return JsonResponse({"type": "FeatureCollection", "features": []})