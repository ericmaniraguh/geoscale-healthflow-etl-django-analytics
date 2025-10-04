
# app/upload_app/urls.py

from xml.etree.ElementInclude import include
from django.urls import path
from .views import dashboard_view
from django.contrib import admin



app_name = 'upload_app'

urlpatterns = [
    # Dashboard views
    path('', dashboard_view.upload_dashboard, name='upload_dashboard'),
    path('dashboard/', dashboard_view.upload_dashboard, name='upload_dashboard'),
    path('user-dashboard/', dashboard_view.user_dashboard, name='user_dashboard'),
    path('admin-dashboard/', dashboard_view.admin_dashboard, name='admin_dashboard'),
    
    # FIXED: Remove double 'upload' prefix - these should match your form actions
    path('shapefile/country/', 
         dashboard_view.upload_country_shapefile, 
         name='upload_country_shapefile'),
    
    path('slope-geojson/', 
         dashboard_view.upload_slope_geojson, 
         name='slope-geojson'),
    
    path('healthcenter-records/', 
         dashboard_view.upload_healthcenter_records, 
         name='upload_healthcenter_malaria_records'),
    
    path('hmis-records/', 
         dashboard_view.upload_hmis_records, 
         name='upload_HMIS_malaria_records'),
    
    path('temperature-data/', 
         dashboard_view.upload_temperature_data, 
         name='upload_temperature_data'),
    
    path('precipitation-data/', 
         dashboard_view.upload_precipitation_data, 
         name='upload_precipitation_data'),
    
    # Test endpoint
    path('test-mongodb/', 
         dashboard_view.test_mongodb_connection, 
         name='test_mongodb_connection'),
]



