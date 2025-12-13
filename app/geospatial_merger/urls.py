from django.urls import path
from . import views

app_name = 'geospatial_merger'

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('upload/', views.upload_files, name='upload_files'),
    path('api/merge/start/', views.start_merge_process, name='api_start_merge'),
    path('api/status/', views.get_processing_status, name='api_status'),
    path('api/stats/global/', views.get_global_stats, name='api_global_stats'),
    path('api/preview/', views.get_results_preview, name='api_preview'),
    path('api/export/', views.export_merged_data, name='api_export'),  # FIXED!
    path('api/check-admin/', views.check_admin_status, name='api_check_admin'),
    path('api/logout/', views.logout_user, name='api_logout'),
    path('access-denied/', views.access_denied_view, name='access_denied'),
]
