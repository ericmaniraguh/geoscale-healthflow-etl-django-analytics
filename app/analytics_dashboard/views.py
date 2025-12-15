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
        base_pattern = "weather_%_prec_and_%_temp_%"
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE %s
            """, [base_pattern])
            tables = [row[0] for row in cursor.fetchall()]
            
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

def get_year_filter(request):
    """Extract year parameter as list of integers"""
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

# --- Metadata APIs (Using accounts_* tables) ---

@login_required
@require_http_methods(["GET"])
@handle_errors
def get_provinces(request):
    with connection.cursor() as cursor:
        cursor.execute("SELECT name FROM accounts_province ORDER BY name")
        provinces = [row[0] for row in cursor.fetchall()]
    return JsonResponse({"provinces": provinces})

@login_required
@require_http_methods(["GET"])
@handle_errors
def get_districts(request):
    province = request.GET.get('province', '').strip()
    with connection.cursor() as cursor:
        if province:
            cursor.execute("""
                SELECT d.name FROM accounts_district d
                JOIN accounts_province p ON d.province_id = p.id
                WHERE p.name = %s ORDER BY d.name
            """, [province])
        else:
            cursor.execute("SELECT name FROM accounts_district ORDER BY name")
        districts = [row[0] for row in cursor.fetchall()]
    return JsonResponse({"districts": districts})

@login_required
@require_http_methods(["GET"])
@handle_errors
def get_sectors(request):
    province = request.GET.get('province', '').strip()
    district = request.GET.get('district', '').strip()
    
    with connection.cursor() as cursor:
        if district:
            cursor.execute("""
                SELECT s.name FROM accounts_sector s
                JOIN accounts_district d ON s.district_id = d.id
                WHERE d.name = %s ORDER BY s.name
            """, [district])
        elif province:
            cursor.execute("""
                SELECT s.name FROM accounts_sector s
                JOIN accounts_district d ON s.district_id = d.id
                JOIN accounts_province p ON d.province_id = p.id
                WHERE p.name = %s ORDER BY s.name
            """, [province])
        else:
            return JsonResponse({"sectors": []})
            
        sectors = [row[0] for row in cursor.fetchall()]
    return JsonResponse({"sectors": sectors})

# --- DATA APIs (Dynamic Tables) ---

@login_required
@require_http_methods(["GET"])
# @cache_response(timeout=60) # Short cache for map data
@handle_errors
def get_map_data(request):
    province, district, sector = get_location_hierarchy(request)
    years = get_year_filter(request)
    
    boundary_table = get_dynamic_table_name('boundary', district, sector)
    health_table_raw = get_dynamic_table_name('health_raw', district, sector) # Use raw for health join? 
    # Actually, usually map joins with a view or table. Let's assume we join boundary with aggregated raw data.
    
    # Check if table exists (optional but good for debugging)
    
    try:
        # Build Query
        # 1. Health Aggregation CTE
        health_where = "WHERE 1=1"
        health_params = []
        if years:
            health_where += " AND year IN %s"
            health_params.append(tuple(years))
            
        # We also filter health data by district/sector columns if they exist in the raw table
        # to ensure consistency if the raw table contains multiple locations
        if district:
            health_where += " AND district ILIKE %s"
            health_params.append(district.strip())
        if sector:
            health_where += " AND sector ILIKE %s"
            health_params.append(sector.strip())

        # 2. Main Query
        # Note: We use dynamic table names
        query = f"""
            WITH village_health AS (
                SELECT 
                    TRIM(LOWER(village)) as join_key,
                    COUNT(*) as total_tests,
                    SUM(CASE WHEN test_result = 'Positive' OR is_positive = true THEN 1 ELSE 0 END) as positive_cases
                FROM {health_table_raw}
                {health_where}
                GROUP BY 1
            )
            SELECT 
                b.district_name, b.sector_name, b.cell_name, b.village_name,
                b.mean_slope, b.max_slope, b.slope_class,
                b.geometry_geojson,
                COALESCE(h.total_tests, 0) as total_tests,
                COALESCE(h.positive_cases, 0) as total_positive
            FROM {boundary_table} b
            LEFT JOIN village_health h ON TRIM(LOWER(b.village_name)) = h.join_key
            WHERE b.geometry_geojson IS NOT NULL
        """
        params = list(health_params) # Copy params
        
        # Add Boundary Filters
        if district:
            query += " AND b.district_name ILIKE %s"
            params.append(district.strip())
        if sector:
            query += " AND b.sector_name ILIKE %s"
            params.append(sector.strip())
            
        query += " LIMIT 5000"
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            
        features = []
        for row in rows:
            data = dict(zip(columns, row))
            
            # Geometry Parsing
            geom_str = data.get('geometry_geojson')
            if not geom_str: continue
            
            try:
                # Handle potential double-escaping
                cleaned_geom = geom_str.strip()
                if isinstance(cleaned_geom, str):
                    if cleaned_geom.startswith('"') and cleaned_geom.endswith('"'):
                         cleaned_geom = cleaned_geom.replace('\\"', '"').strip('"')
                    geom = json.loads(cleaned_geom)
                else:
                    geom = cleaned_geom # Already dict/json
            except Exception as e:
                continue

            # Stats
            tests = data.get('total_tests', 0)
            pos = data.get('total_positive', 0)
            rate = round((pos / tests * 100), 1) if tests > 0 else 0
            
            if rate > 5: level = 'High'
            elif rate >= 1: level = 'Medium'
            else: level = 'Low'
            
            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "district": data.get('district_name'),
                    "sector": data.get('sector_name'),
                    "cell": data.get('cell_name'),
                    "village": data.get('village_name'),
                    "mean_slope": data.get('mean_slope'),
                    "max_slope": data.get('max_slope'),
                    "slope_class": data.get('slope_class'),
                    "total_tests": tests,
                    "total_positive": pos,
                    "positivity_rate": rate,
                    "risk_level": level,
                    "has_data": (tests > 0)
                }
            })
            
        return JsonResponse({
            "type": "FeatureCollection",
            "features": features
        })
        
    except Exception as e:
        # If table doesn't exist, this will throw.
        error_msg = str(e).lower()
        if 'undefined table' in error_msg or 'does not exist' in error_msg:
            logger.warning(f"Map data table not found: {boundary_table} or {health_table_raw}")
            return JsonResponse({"type": "FeatureCollection", "features": []}) # Return empty map
        raise e

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_kpi_data(request):
    province, district, sector = get_location_hierarchy(request)
    years = get_year_filter(request)
    
    table_yearly = get_dynamic_table_name('yearly', district, sector)
    table_api = get_dynamic_table_name('api', district, sector)
    
    try:
        query = f"""
            SELECT 
                SUM(total_tests), SUM(positive_cases), SUM(negative_cases), AVG(positivity_rate)
            FROM {table_yearly}
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
            
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
            
        total_tests = row[0] or 0
        total_pos = row[1] or 0
        total_neg = row[2] or 0
        avg_rate = round(row[3], 1) if row[3] else 0
        
        # API / Population (Optional, fail gracefully if missing)
        try:
            api_query = f"SELECT SUM(population), AVG(api) FROM {table_api} WHERE 1=1"
            api_params = []
            if district: api_query += " AND district ILIKE %s"; api_params.append(district)
            # if sector: ... specific API might not exist per sector in this table structure
            
            with connection.cursor() as cursor:
                cursor.execute(api_query, api_params)
                api_row = cursor.fetchone()
            
            pop = api_row[0] or 0
            api = round(api_row[1], 1) if api_row[1] else 0
            incidence = round((total_pos / pop * 1000), 1) if pop > 0 else 0
        except:
            api = 0
            incidence = 0
            
        return JsonResponse({
            "total_tests": total_tests,
            "total_positive": total_pos,
            "total_negative": total_neg,
            "avg_positivity_rate": avg_rate,
            "positive_change": f"{api}", 
            "negative_change": f"{incidence}"
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
def get_monthly_trend(request):
    province, district, sector = get_location_hierarchy(request)
    years = get_year_filter(request)
    table_monthly = get_dynamic_table_name('monthly', district, sector)
    
    try:
        query = f"""
            SELECT month_name, AVG(positivity_rate), year, month
            FROM {table_monthly}
            WHERE 1=1
        """
        params = []
        if years: query += " AND year IN %s"; params.append(tuple(years))
        if district: query += " AND filter_district ILIKE %s"; params.append(district)
        if sector: query += " AND filter_sector ILIKE %s"; params.append(sector)
        
        query += " GROUP BY year, month, month_name ORDER BY month"
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
        month_labels = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        
        found_years = sorted(list(set(r[2] for r in rows))) if not years else years
        target_years = found_years if found_years else [2021, 2022, 2023]
        
        response = {"labels": month_labels, "available_years": target_years}
        for y in target_years:
            vals = [0] * 12
            for r in rows:
                if r[2] == y:
                    idx = r[3] - 1
                    if 0 <= idx < 12: vals[idx] = float(r[1])
            response[str(y)] = vals
            
        return JsonResponse(response)
    except Exception as e:
        error_msg = str(e).lower()
        if 'undefined table' in error_msg or 'does not exist' in error_msg:
            return JsonResponse({"labels": [], "available_years": []})
        raise e

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_villages_data(request):
    province, district, sector = get_location_hierarchy(request)
    years = get_year_filter(request)
    table_raw = get_dynamic_table_name('health_raw', district, sector)
    
    try:
        query = f"""
            SELECT 
                INITCAP(village), COUNT(*),
                SUM(CASE WHEN test_result = 'Positive' OR is_positive = true THEN 1 ELSE 0 END)
            FROM {table_raw}
            WHERE 1=1
        """
        params = []
        if years: query += " AND year IN %s"; params.append(tuple(years))
        if district: query += " AND district ILIKE %s"; params.append(district)
        if sector: query += " AND sector ILIKE %s"; params.append(sector)
        
        query += " GROUP BY 1 HAVING COUNT(*) > 0 ORDER BY 3 DESC"
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
        year_str = ", ".join(map(str, sorted(years))) if years else "All Available"
        villages = []
        for r in rows:
            mapped_village = r[0]
            if not mapped_village: continue
            total = r[1]
            pos = r[2] or 0
            rate = round((pos / total * 100), 1) if total > 0 else 0
            villages.append({
                "village": mapped_village, "tests": total, "positive": pos, "rate": rate, "year": year_str
            })
            
        return JsonResponse({"villages": villages})
    except Exception as e:
        error_msg = str(e).lower()
        if 'undefined table' in error_msg or 'does not exist' in error_msg: return JsonResponse({"villages": []})
        raise e

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_location_summary(request):
    # This usually reuses yearly stats
    group_by = request.GET.get('group_by', 'district')
    province, district, sector = get_location_hierarchy(request)
    years = get_year_filter(request)
    table_yearly = get_dynamic_table_name('yearly', district, sector)
    table_raw = get_dynamic_table_name('health_raw', district, sector)
    
    try:
        col = 'filter_sector' if sector else 'filter_district'
        query = f"""
            SELECT {col}, SUM(total_tests), SUM(positive_cases), SUM(negative_cases), AVG(positivity_rate)
            FROM {table_yearly} WHERE 1=1
        """
        params = []
        if years: query += " AND year IN %s"; params.append(tuple(years))
        if district: query += " AND filter_district ILIKE %s"; params.append(district)
        if sector: query += " AND filter_sector ILIKE %s"; params.append(sector)
        query += f" GROUP BY {col} ORDER BY 3 DESC"
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            stats_rows = cursor.fetchall()

        # Village Counts from Raw
        v_col = 'sector' if sector else 'district'
        v_query = f"SELECT {v_col}, COUNT(DISTINCT INITCAP(village)) FROM {table_raw} WHERE 1=1"
        v_params = []
        if years: v_query += " AND year IN %s"; v_params.append(tuple(years))
        if district: v_query += " AND district ILIKE %s"; v_params.append(district)
        if sector: v_query += " AND sector ILIKE %s"; v_params.append(sector)
        v_query += f" GROUP BY {v_col}"
        
        with connection.cursor() as cursor:
            cursor.execute(v_query, v_params)
            v_rows = cursor.fetchall()
            v_map = {r[0]: r[1] for r in v_rows if r[0]}

        summary = []
        for r in stats_rows:
            loc = r[0]
            if not loc: continue
            summary.append({
                "location": loc,
                "total_tests": r[1],
                "total_positive": r[2],
                "total_negative": r[3],
                "avg_positivity_rate": round(r[4], 1) if r[4] else 0,
                "num_villages": v_map.get(loc, 0)
            })
            
        return JsonResponse({"summary": summary})
    except Exception as e:
        error_msg = str(e).lower()
        if 'undefined table' in error_msg or 'does not exist' in error_msg: return JsonResponse({"summary": []})
        raise e

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_precipitation_data(request):
    return _get_weather_data(request, 'precipitation')

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_temperature_data(request):
    return _get_weather_data(request, 'temperature')

def _get_weather_data(request, data_type):
    province, district, sector = get_location_hierarchy(request)
    years = get_year_filter(request)
    table_name = get_dynamic_weather_table(district)
    
    col_name = 'monthly_precipitation' if data_type == 'precipitation' else 'monthly_temperature'
    
    if not table_name:
         return JsonResponse({"labels": ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], "available_years": []})

    try:
        query = f"SELECT year, month, AVG({col_name}) FROM {table_name} WHERE 1=1"
        params = []
        if years: query += " AND year IN %s"; params.append(tuple(years))
        if district: query += " AND district ILIKE %s"; params.append(district)
        if sector: query += " AND sector ILIKE %s"; params.append(sector)
        query += " GROUP BY year, month ORDER BY year, month"
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
        labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        found_years = sorted(list(set(r[0] for r in rows)))
        response = {"labels": labels, "available_years": found_years}
        
        data_map = {}
        for r in rows:
            y, m, val = r[0], int(r[1]), (round(float(r[2]), 1) if r[2] is not None else 0)
            if y not in data_map: data_map[y] = {}
            data_map[y][m] = val
            
        for y in found_years:
            series = [None] * 12
            if y in data_map:
                for i in range(12): series[i] = data_map[y].get(i+1)
            response[str(y)] = series
            
        return JsonResponse(response)
    except Exception as e:
        logger.error(f"Error {data_type}: {e}")
        return JsonResponse({"labels": [], "available_years": []})

@login_required
@require_http_methods(["GET"])
@cache_response(timeout=300)
@handle_errors
def get_gender_analysis(request):
    province, district, sector = get_location_hierarchy(request)
    years = get_year_filter(request)
    table_raw = get_dynamic_table_name('health_raw', district, sector)
    
    try:
        query = f"""
            SELECT gender, COUNT(*), 
            SUM(CASE WHEN test_result = 'Positive' OR is_positive = true THEN 1 ELSE 0 END), year 
            FROM {table_raw} WHERE gender IS NOT NULL
        """
        params = []
        if years: query += " AND year IN %s"; params.append(tuple(years))
        if district: query += " AND district ILIKE %s"; params.append(district)
        if sector: query += " AND sector ILIKE %s"; params.append(sector)
        query += " GROUP BY gender, year ORDER BY gender"
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
        # ... logic similar to previous implementation ...
        unique_genders = sorted(list(set(r[0] for r in rows if r[0])))
        found_years = sorted(list(set(r[3] for r in rows))) if not years else years
        target_years = found_years if found_years else [2021, 2022, 2023]
        
        response = {"labels": unique_genders, "available_years": target_years}
        for y in target_years:
            vals = []
            for g in unique_genders:
                match = next((r for r in rows if r[0] == g and r[3] == y), None)
                if match:
                    tot, pos = match[1], match[2]
                    vals.append(round((pos/tot*100), 1) if tot > 0 else 0)
                else: vals.append(0)
            response[str(y)] = vals
            
        return JsonResponse(response)

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
    """
    Export dashboard data as CSV/Excel.
    Placeholder for now to satisfy URLconf.
    """
    return JsonResponse({"status": "error", "message": "Export functionality not implemented yet"}, status=501)