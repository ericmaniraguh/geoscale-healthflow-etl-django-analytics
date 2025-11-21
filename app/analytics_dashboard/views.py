"""
Enhanced Geospatial Analytics Views - Database Location Integration
Pulls Province, District, Sector from accounts app models

app/analytics_dashboard/views.py
"""

from django.shortcuts import render
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_http_methods
from django.db.models import Q, Sum, Avg
from django.apps import apps
import logging
from functools import wraps
import json

logger = logging.getLogger(__name__)

from .models import (
    HealthCenterLocation,
    MalariaAnalyticsAggregated,
    LocationHierarchyCache,
    DashboardCache
)


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def build_location_filter(request):
    """Build Q filter from location parameters (province, district, sector)"""
    filters = Q()
    
    province_name = request.GET.get('province', '').strip()
    district_name = request.GET.get('district', '').strip()
    sector_name = request.GET.get('sector', '').strip()
    
    if sector_name:
        filters &= Q(health_centre__sector__name=sector_name)
    elif district_name:
        filters &= Q(health_centre__sector__district__name=district_name)
    elif province_name:
        filters &= Q(health_centre__sector__district__province__name=province_name)
    
    return filters


def build_year_range_filter(request):
    """Build Q filter for year range (year_from, year_to)"""
    filters = Q()
    
    try:
        year_from = int(request.GET.get('year_from', ''))
        year_to = int(request.GET.get('year_to', ''))
        
        if year_from and year_to:
            filters &= Q(year__gte=year_from, year__lte=year_to)
        elif year_from:
            filters &= Q(year__gte=year_from)
        elif year_to:
            filters &= Q(year__lte=year_to)
    except (ValueError, TypeError):
        pass
    
    return filters


# ═══════════════════════════════════════════════════════════════════════════════
# DECORATORS
# ═══════════════════════════════════════════════════════════════════════════════

def cached_api(timeout=600):
    """Cache API responses"""
    def decorator(func):
        @wraps(func)
        def wrapper(request, *args, **kwargs):
            query_string = request.GET.urlencode()
            cache_key = f"api_{func.__name__}_{query_string}"
            
            cached_data = DashboardCache.objects.filter(cache_key=cache_key).first()
            if cached_data and not cached_data.is_expired():
                cached_data.cache_data['cached'] = True
                return JsonResponse(cached_data.cache_data)
            
            response = func(request, *args, **kwargs)
            
            if response.status_code == 200:
                data = response.json()
                DashboardCache.objects.update_or_create(
                    cache_key=cache_key,
                    defaults={
                        'cache_data': data,
                        'expires_at': __import__('django.utils.timezone', fromlist=['now']).now() + 
                                     __import__('datetime', fromlist=['timedelta']).timedelta(seconds=timeout)
                    }
                )
            
            return response
        return wrapper
    return decorator


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE VIEWS
# ═══════════════════════════════════════════════════════════════════════════════

@login_required
def geospatial_dashboard(request):
    """Main geospatial analytics dashboard"""
    context = {
        'page_title': 'Geospatial Analytics Dashboard',
        'user': request.user,
    }
    return render(request, 'analytics_dashboard/geospatial_dashboard.html', context)


# ═══════════════════════════════════════════════════════════════════════════════
# FILTER ENDPOINTS - DATABASE INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════════

@login_required
@require_http_methods(["GET"])
@cached_api(timeout=3600)
def get_filter_options(request):
    """
    Get all available filter options from DATABASE (not hardcoded)
    Uses accounts app models: Province, District, Sector
    """
    try:
        # Get models from accounts app
        Province = apps.get_model('accounts', 'Province')
        District = apps.get_model('accounts', 'District')
        Sector = apps.get_model('accounts', 'Sector')
        
        # Get all provinces from database
        provinces = list(
            Province.objects.all()
            .values_list('name', flat=True)
            .order_by('name')
        )
        
        logger.info(f"Loaded {len(provinces)} provinces from database")
        
        # Get districts organized by province
        districts = {}
        for province_name in provinces:
            try:
                province = Province.objects.get(name=province_name)
                dists = list(
                    District.objects.filter(province=province)
                    .values_list('name', flat=True)
                    .order_by('name')
                )
                districts[province_name] = dists
            except Province.DoesNotExist:
                logger.warning(f"Province not found: {province_name}")
                districts[province_name] = []
        
        # Get sectors organized by district
        sectors = {}
        for province_name in provinces:
            try:
                province = Province.objects.get(name=province_name)
                for dist_name in districts.get(province_name, []):
                    try:
                        district = District.objects.get(province=province, name=dist_name)
                        sects = list(
                            Sector.objects.filter(district=district)
                            .values_list('name', flat=True)
                            .order_by('name')
                        )
                        sectors[dist_name] = sects
                    except District.DoesNotExist:
                        logger.warning(f"District not found: {dist_name}")
                        sectors[dist_name] = []
            except Province.DoesNotExist:
                pass
        
        # Get available years from analytics data
        years = list(
            MalariaAnalyticsAggregated.objects
            .values_list('year', flat=True)
            .distinct()
            .order_by('-year')
        )
        
        return JsonResponse({
            'success': True,
            'provinces': provinces,
            'districts': districts,
            'sectors': sectors,
            'years': years,
            'months': list(range(1, 13)),
            'source': 'database'  # Indicate data comes from database
        })
    
    except Exception as e:
        logger.error(f"Error fetching filter options from database: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e),
            'source': 'database'
        }, status=500)


@login_required
@require_http_methods(["GET"])
@cached_api(timeout=600)
def get_districts(request):
    """Get districts for selected province from DATABASE"""
    province_name = request.GET.get('province', '').strip()
    
    if not province_name:
        return JsonResponse({
            'success': False,
            'error': 'Province parameter required'
        }, status=400)
    
    try:
        Province = apps.get_model('accounts', 'Province')
        District = apps.get_model('accounts', 'District')
        
        # Get province from database
        province = Province.objects.filter(name=province_name).first()
        if not province:
            return JsonResponse({
                'success': False,
                'error': f'Province {province_name} not found in database'
            }, status=404)
        
        # Get districts for this province
        districts = list(
            District.objects
            .filter(province=province)
            .values_list('name', flat=True)
            .order_by('name')
        )
        
        logger.info(f"Loaded {len(districts)} districts for province: {province_name}")
        
        return JsonResponse({
            'success': True,
            'province': province_name,
            'districts': districts,
            'count': len(districts),
            'source': 'database'
        })
    
    except Exception as e:
        logger.error(f"Error fetching districts from database: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e),
            'source': 'database'
        }, status=500)


@login_required
@require_http_methods(["GET"])
@cached_api(timeout=600)
def get_sectors(request):
    """Get sectors for selected district from DATABASE"""
    province_name = request.GET.get('province', '').strip()
    district_name = request.GET.get('district', '').strip()
    
    if not province_name or not district_name:
        return JsonResponse({
            'success': False,
            'error': 'Province and district parameters required'
        }, status=400)
    
    try:
        Province = apps.get_model('accounts', 'Province')
        District = apps.get_model('accounts', 'District')
        Sector = apps.get_model('accounts', 'Sector')
        
        # Get province from database
        province = Province.objects.filter(name=province_name).first()
        if not province:
            return JsonResponse({'success': False, 'error': 'Province not found'}, status=404)
        
        # Get district from database
        district = District.objects.filter(province=province, name=district_name).first()
        if not district:
            return JsonResponse({'success': False, 'error': 'District not found'}, status=404)
        
        # Get sectors for this district
        sectors = list(
            Sector.objects
            .filter(district=district)
            .values_list('name', flat=True)
            .order_by('name')
        )
        
        logger.info(f"Loaded {len(sectors)} sectors for district: {district_name}")
        
        return JsonResponse({
            'success': True,
            'province': province_name,
            'district': district_name,
            'sectors': sectors,
            'count': len(sectors),
            'source': 'database'
        })
    
    except Exception as e:
        logger.error(f"Error fetching sectors from database: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e),
            'source': 'database'
        }, status=500)


@login_required
@require_http_methods(["GET"])
@cached_api(timeout=600)
def get_health_centres(request):
    """Get health centres for selected sector from DATABASE"""
    province_name = request.GET.get('province', '').strip()
    district_name = request.GET.get('district', '').strip()
    sector_name = request.GET.get('sector', '').strip()
    
    if not all([province_name, district_name, sector_name]):
        return JsonResponse({
            'success': False,
            'error': 'Province, district, and sector parameters required'
        }, status=400)
    
    try:
        Province = apps.get_model('accounts', 'Province')
        District = apps.get_model('accounts', 'District')
        Sector = apps.get_model('accounts', 'Sector')
        
        # Navigate hierarchy from database
        province = Province.objects.filter(name=province_name).first()
        if not province:
            return JsonResponse({'success': False, 'error': 'Province not found'}, status=404)
        
        district = District.objects.filter(province=province, name=district_name).first()
        if not district:
            return JsonResponse({'success': False, 'error': 'District not found'}, status=404)
        
        sector = Sector.objects.filter(district=district, name=sector_name).first()
        if not sector:
            return JsonResponse({'success': False, 'error': 'Sector not found'}, status=404)
        
        # Get health centres from this sector
        health_centres = list(
            HealthCenterLocation.objects
            .filter(sector=sector, is_active=True)
            .values('id', 'code', 'name')
            .order_by('name')
        )
        
        logger.info(f"Loaded {len(health_centres)} health centres for sector: {sector_name}")
        
        return JsonResponse({
            'success': True,
            'province': province_name,
            'district': district_name,
            'sector': sector_name,
            'health_centres': health_centres,
            'count': len(health_centres),
            'source': 'database'
        })
    
    except Exception as e:
        logger.error(f"Error fetching health centres from database: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e),
            'source': 'database'
        }, status=500)


# ═══════════════════════════════════════════════════════════════════════════════
# SUMMARY STATISTICS
# ═══════════════════════════════════════════════════════════════════════════════

@login_required
@require_http_methods(["GET"])
@cached_api(timeout=300)
def get_summary_stats(request):
    """Get summary statistics with year range filtering"""
    try:
        location_filter = build_location_filter(request)
        year_filter = build_year_range_filter(request)
        
        combined_filter = location_filter & year_filter
        
        stats = MalariaAnalyticsAggregated.objects \
            .filter(combined_filter) \
            .aggregate(
                total_tests=Sum('total_tests'),
                positive_cases=Sum('positive_cases'),
                negative_cases=Sum('negative_cases'),
            )
        
        total_tests = stats['total_tests'] or 0
        positive_cases = stats['positive_cases'] or 0
        
        positivity_rate = (positive_cases / total_tests * 100) if total_tests > 0 else 0
        
        # Count unique health centres
        health_centres_count = HealthCenterLocation.objects \
            .filter(combined_filter if combined_filter else Q()) \
            .distinct().count()
        
        return JsonResponse({
            'success': True,
            'data': {
                'total_tests': total_tests,
                'positive_cases': positive_cases,
                'negative_cases': stats['negative_cases'] or 0,
                'positivity_rate': round(positivity_rate, 2),
                'health_centres_count': health_centres_count,
            }
        })
    
    except Exception as e:
        logger.error(f"Error fetching summary stats: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


# ═══════════════════════════════════════════════════════════════════════════════
# GEOSPATIAL DATA
# ═══════════════════════════════════════════════════════════════════════════════

@login_required
@require_http_methods(["GET"])
@cached_api(timeout=600)
def get_map_data(request):
    """Get map data with year range filtering"""
    try:
        location_filter = build_location_filter(request)
        year_filter = build_year_range_filter(request)
        combined_filter = location_filter & year_filter
        
        # Get aggregated analytics
        analytics_data = MalariaAnalyticsAggregated.objects \
            .filter(combined_filter) \
            .values('health_centre_id') \
            .annotate(
                total_tests=Sum('total_tests'),
                positive_cases=Sum('positive_cases'),
                avg_positivity=Avg('positivity_rate'),
            )
        
        analytics_lookup = {
            item['health_centre_id']: item 
            for item in analytics_data
        }
        
        # Get health centres
        health_centres = HealthCenterLocation.objects \
            .filter(is_active=True) \
            .values('id', 'code', 'name', 'latitude', 'longitude')
        
        features = []
        for hc in health_centres:
            if hc['id'] not in analytics_lookup:
                continue
            
            analytics = analytics_lookup[hc['id']]
            
            feature = {
                'type': 'Feature',
                'id': hc['code'],
                'geometry': {
                    'type': 'Point',
                    'coordinates': [hc['longitude'], hc['latitude']]
                },
                'properties': {
                    'name': hc['name'],
                    'code': hc['code'],
                    'total_tests': analytics['total_tests'] or 0,
                    'positive_cases': analytics['positive_cases'] or 0,
                    'positivity_rate': analytics['avg_positivity'] or 0,
                }
            }
            features.append(feature)
        
        return JsonResponse({
            'success': True,
            'type': 'FeatureCollection',
            'features': features,
            'count': len(features)
        })
    
    except Exception as e:
        logger.error(f"Error fetching map data: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@login_required
@require_http_methods(["GET"])
@cached_api(timeout=600)
def get_slope_map_data(request):
    """Get slope-based map visualization"""
    try:
        location_filter = build_location_filter(request)
        year_filter = build_year_range_filter(request)
        combined_filter = location_filter & year_filter
        
        analytics_data = MalariaAnalyticsAggregated.objects \
            .filter(combined_filter) \
            .values('health_centre_id') \
            .annotate(
                total_tests=Sum('total_tests'),
                positive_cases=Sum('positive_cases'),
                avg_positivity=Avg('positivity_rate'),
            )
        
        analytics_lookup = {
            item['health_centre_id']: item 
            for item in analytics_data
        }
        
        health_centres = HealthCenterLocation.objects \
            .filter(is_active=True) \
            .values('id', 'code', 'name', 'latitude', 'longitude', 'slope_percent')
        
        features = []
        for hc in health_centres:
            if hc['id'] not in analytics_lookup:
                continue
            
            slope = hc['slope_percent'] or 0
            
            # Color by slope
            if slope < 5:
                color = '#27ae60'
            elif slope < 15:
                color = '#f39c12'
            elif slope < 30:
                color = '#e67e22'
            else:
                color = '#e74c3c'
            
            feature = {
                'type': 'Feature',
                'id': hc['code'],
                'geometry': {
                    'type': 'Point',
                    'coordinates': [hc['longitude'], hc['latitude']]
                },
                'properties': {
                    'name': hc['name'],
                    'code': hc['code'],
                    'slope_percent': slope,
                    'color': color,
                }
            }
            features.append(feature)
        
        return JsonResponse({
            'success': True,
            'type': 'FeatureCollection',
            'features': features,
            'count': len(features),
            'slope_classes': {
                '0-5%': 'Level',
                '5-15%': 'Gently sloping',
                '15-30%': 'Moderately sloping',
                '>30%': 'Steeply sloping'
            }
        })
    
    except Exception as e:
        logger.error(f"Error fetching slope map data: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSIS ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@login_required
@require_http_methods(["GET"])
@cached_api(timeout=600)
def get_gender_analysis(request):
    """Get gender-based analysis with year range filtering"""
    try:
        location_filter = build_location_filter(request)
        year_filter = build_year_range_filter(request)
        combined_filter = location_filter & year_filter
        
        gender_data = MalariaAnalyticsAggregated.objects \
            .filter(combined_filter) \
            .aggregate(
                female_tests=Sum('female_tests'),
                female_positive=Sum('female_positive'),
                male_tests=Sum('male_tests'),
                male_positive=Sum('male_positive'),
            )
        
        female_tests = gender_data['female_tests'] or 0
        female_positive = gender_data['female_positive'] or 0
        male_tests = gender_data['male_tests'] or 0
        male_positive = gender_data['male_positive'] or 0
        
        female_rate = (female_positive / female_tests * 100) if female_tests > 0 else 0
        male_rate = (male_positive / male_tests * 100) if male_tests > 0 else 0
        
        return JsonResponse({
            'success': True,
            'data': {
                'female': {
                    'total_tests': female_tests,
                    'positive': female_positive,
                    'positivity_rate': round(female_rate, 2)
                },
                'male': {
                    'total_tests': male_tests,
                    'positive': male_positive,
                    'positivity_rate': round(male_rate, 2)
                }
            }
        })
    
    except Exception as e:
        logger.error(f"Error fetching gender analysis: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@login_required
@require_http_methods(["GET"])
@cached_api(timeout=600)
def get_temporal_trends(request):
    """Get temporal trends with year range filtering"""
    try:
        location_filter = build_location_filter(request)
        year_filter = build_year_range_filter(request)
        combined_filter = location_filter & year_filter
        
        trend_type = request.GET.get('type', 'yearly').strip()
        
        month_names = {
            1: 'January', 2: 'February', 3: 'March', 4: 'April',
            5: 'May', 6: 'June', 7: 'July', 8: 'August',
            9: 'September', 10: 'October', 11: 'November', 12: 'December'
        }
        
        if trend_type == 'monthly':
            trends = MalariaAnalyticsAggregated.objects \
                .filter(combined_filter) \
                .values('year', 'month') \
                .annotate(
                    total_tests=Sum('total_tests'),
                    positive_cases=Sum('positive_cases'),
                    avg_positivity=Avg('positivity_rate'),
                ) \
                .order_by('year', 'month')
            
            data = []
            for trend in trends:
                positive = trend['positive_cases'] or 0
                tests = trend['total_tests'] or 0
                rate = (positive / tests * 100) if tests > 0 else 0
                
                data.append({
                    'year': trend['year'],
                    'month': trend['month'],
                    'month_name': month_names.get(trend['month'], ''),
                    'total_tests': tests,
                    'positive_cases': positive,
                    'positivity_rate': round(rate, 2)
                })
        
        else:  # yearly
            trends = MalariaAnalyticsAggregated.objects \
                .filter(combined_filter) \
                .values('year') \
                .annotate(
                    total_tests=Sum('total_tests'),
                    positive_cases=Sum('positive_cases'),
                    avg_positivity=Avg('positivity_rate'),
                ) \
                .order_by('-year')
            
            data = []
            for trend in trends:
                positive = trend['positive_cases'] or 0
                tests = trend['total_tests'] or 0
                rate = (positive / tests * 100) if tests > 0 else 0
                
                data.append({
                    'year': trend['year'],
                    'total_tests': tests,
                    'positive_cases': positive,
                    'positivity_rate': round(rate, 2)
                })
        
        return JsonResponse({
            'success': True,
            'type': trend_type,
            'data': data,
            'count': len(data)
        })
    
    except Exception as e:
        logger.error(f"Error fetching temporal trends: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@login_required
@require_http_methods(["GET"])
@cached_api(timeout=600)
def get_environmental_data(request):
    """Get environmental data (temperature, precipitation) with year range filtering"""
    try:
        location_filter = build_location_filter(request)
        year_filter = build_year_range_filter(request)
        combined_filter = location_filter & year_filter
        
        # Get aggregated environmental data by month
        env_data = MalariaAnalyticsAggregated.objects \
            .filter(combined_filter) \
            .values('month') \
            .annotate(
                avg_temperature=Avg('temperature_avg'),
                avg_precipitation=Avg('precipitation_mm'),
            ) \
            .order_by('month')
        
        month_names = {
            1: 'January', 2: 'February', 3: 'March', 4: 'April',
            5: 'May', 6: 'June', 7: 'July', 8: 'August',
            9: 'September', 10: 'October', 11: 'November', 12: 'December'
        }
        
        data = []
        for item in env_data:
            if item['month']:
                data.append({
                    'month': item['month'],
                    'month_name': month_names.get(item['month'], ''),
                    'temperature_avg': round(item['avg_temperature'] or 0, 2),
                    'precipitation_mm': round(item['avg_precipitation'] or 0, 2),
                })
        
        return JsonResponse({
            'success': True,
            'data': data,
            'count': len(data)
        })
    
    except Exception as e:
        logger.error(f"Error fetching environmental data: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


# ═══════════════════════════════════════════════════════════════════════════════
# CACHE MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

@login_required
@require_http_methods(["POST"])
def clear_dashboard_cache(request):
    """Clear all dashboard caches"""
    try:
        DashboardCache.objects.all().delete()
        logger.info("Dashboard cache cleared")
        
        return JsonResponse({
            'success': True,
            'message': 'Cache cleared successfully'
        })
    
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)