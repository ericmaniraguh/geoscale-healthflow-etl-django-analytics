from django.apps import AppConfig

class EtlAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'app.etl_app'
    label = 'etl_app'
