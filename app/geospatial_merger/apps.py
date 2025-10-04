# 1. app/geospatial_merger/apps.py

from django.apps import AppConfig

class GeospatialMergerConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'app.geospatial_merger'
    verbose_name = 'Geospatial Merger'
