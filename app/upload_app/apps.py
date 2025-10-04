# app/upload_app/apps.py
from django.apps import AppConfig

class UploadAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'app.upload_app'  # This should match your Python import path
    label = 'upload_app'     # This should be unique