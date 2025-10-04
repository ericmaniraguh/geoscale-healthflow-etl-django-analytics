# app/upload_app/handlers/__init__.py

# Make handler functions easily importable
from .upload_handlers import (
    handle_shapefile_upload,
    handle_slope_upload,
    handle_healthcenter_upload,
    handle_hmis_upload,
    handle_temperature_upload,
    handle_precipitation_upload,
)

__all__ = [
    'handle_shapefile_upload',
    'handle_slope_upload', 
    'handle_healthcenter_upload',
    'handle_hmis_upload',
    'handle_temperature_upload',
    'handle_precipitation_upload',
]