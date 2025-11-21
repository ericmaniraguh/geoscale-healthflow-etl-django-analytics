"""
Django URLs Configuration - Enhanced
Rwanda Malaria Surveillance Dashboard with Geospatial Analysis
Integrates Airflow data pipeline with mapping and clustering
Year Range Filtering Support
"""

from django.urls import path
from . import views

app_name = 'analytics_dashboard'

urlpatterns = [
    # ===== MAIN DASHBOARD VIEW =====
    path('', views.geospatial_dashboard, name='dashboard'),
    
    # ===== LOCATION FILTERING APIS =====
    path('api/filters/', 
         views.get_filter_options, 
         name='api_filters'),
    
    path('api/districts/', 
         views.get_districts, 
         name='api_districts'),
    
    path('api/sectors/', 
         views.get_sectors, 
         name='api_sectors'),
    
    path('api/health-centres/', 
         views.get_health_centres, 
         name='api_health_centres'),
    
    # ===== SUMMARY STATISTICS =====
    path('api/summary/', 
         views.get_summary_stats, 
         name='api_summary'),
    
    # ===== GEOSPATIAL DATA FOR MAPS =====
    path('api/map/', 
         views.get_map_data, 
         name='api_map'),
    
    path('api/slope-map/', 
         views.get_slope_map_data, 
         name='api_slope_map'),
    
    # ===== ANALYSIS ENDPOINTS =====
    path('api/gender/', 
         views.get_gender_analysis, 
         name='api_gender'),
    
    path('api/trends/', 
         views.get_temporal_trends, 
         name='api_trends'),
    
    path('api/environmental/', 
         views.get_environmental_data, 
         name='api_environmental'),
    
    # ===== CACHE MANAGEMENT =====
    path('api/cache/clear/', 
         views.clear_dashboard_cache, 
         name='api_cache_clear'),
]