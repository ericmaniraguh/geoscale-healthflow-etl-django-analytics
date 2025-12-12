# """
# Analytics Dashboard URLs
# app/analytics_dashboard/urls.py

# """

# from django.urls import path
# from . import views

# app_name = 'analytics_dashboard'

# urlpatterns = [
#     # Main Dashboard View
#     path('', views.dashboard, name='dashboard'),
    
#     # API Endpoints for Dashboard Data
#     path('api/kpi/', views.get_kpi_data, name='api_kpi'),
#     path('api/gender/', views.get_gender_analysis, name='api_gender'),
#     path('api/precipitation/', views.get_precipitation_data, name='api_precipitation'),
#     path('api/monthly-trend/', views.get_monthly_trend, name='api_monthly_trend'),
#     path('api/villages/', views.get_villages_data, name='api_villages'),
    
#     # Additional API endpoints (if needed)
#     path('api/refresh/', views.refresh_data, name='api_refresh'),
#     path('api/export/', views.export_data, name='api_export'),
# ]

from django.urls import path
from . import views

app_name = 'analytics_dashboard'

urlpatterns = [
    # Main Dashboard View
    path('', views.dashboard, name='dashboard'),
    
    # NEW: Location metadata endpoints
    path('api/locations/provinces/', views.get_provinces, name='api_provinces'),
    path('api/locations/districts/', views.get_districts, name='api_districts'),
    path('api/locations/sectors/', views.get_sectors, name='api_sectors'),
    
    # API Endpoints for Dashboard Data
    path('api/kpi/', views.get_kpi_data, name='api_kpi'),
    path('api/gender/', views.get_gender_analysis, name='api_gender'),
    path('api/precipitation/', views.get_precipitation_data, name='precipitation_data'),
    path('api/temperature/', views.get_temperature_data, name='temperature_data'),
    path('api/monthly-trend/', views.get_monthly_trend, name='monthly_trend'),
    path('api/villages/', views.get_villages_data, name='api_villages'),
    
    # NEW: Location summary endpoint
    path('api/location-summary/', views.get_location_summary, name='api_location_summary'),
    
    # NEW: Map Data endpoint
    path('api/map-data/', views.get_map_data, name='api_map_data'),
    
    # Additional API endpoints
    path('api/refresh/', views.refresh_data, name='api_refresh'),
    path('api/export/', views.export_data, name='api_export'),
]