

from django.urls import path

from .views.malaria_api_calculator_etl_view import MalariaAPICalculatorView
from .views.health_center_lab_view import HealthCenterLabDataETLView
from .views.weather_data_prec_temp_etl_view import WeatherDataETLView
from .views.geoJson_slope_etl_view import SlopeGeoJsonETLView
from .views.village_admin_boundaries_etl_view import VillageAdminBoundariesETLView
from .views.etl_dashboard_view import etl_dashboard, test_etl_apis

app_name = 'etl_app'  # namespace

urlpatterns = [
    # === Dashboard ===
    path('', etl_dashboard, name='etl_dashboard'),     
    path('dashboard/', etl_dashboard, name='etl_dashboard_alt'),
    path('test-apis/', test_etl_apis, name='test_apis'),

    # === ETL endpoints ===
    path('api/malaria/calculate/', MalariaAPICalculatorView.as_view(), name='malaria_api_calculator'),
    path('hc/lab-data/', HealthCenterLabDataETLView.as_view(), name='hc-lab-etl'),
    path('weather/prec-temp/data/', WeatherDataETLView.as_view(), name='weather-data-etl'),
    path('shapefile/admin-boundaries/', VillageAdminBoundariesETLView.as_view(), name='shapefile-admin-boundaries-etl'),
    path('extract/slope-geojson/', SlopeGeoJsonETLView.as_view(), name='extract-slope-geojson'),
]
