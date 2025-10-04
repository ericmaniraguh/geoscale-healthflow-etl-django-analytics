
# 1. MAIN PROJECT URLs (main_project/urls.py) - UPDATED
# =============================================================================

from django.contrib import admin
from django.urls import path, include
from django.shortcuts import redirect

def redirect_to_dashboard(request):
    if request.user.is_authenticated:
        # Check if user is admin for geospatial app
        if request.user.is_staff or request.user.is_superuser:
            return redirect('geospatial_merger:dashboard')  # FIXED: Redirect to geospatial dashboard
        else:
            return redirect('etl_app:etl_dashboard')  # Fallback to ETL dashboard
    else:
        return redirect('accounts:login')

urlpatterns = [
    # Django admin
    path('admin/', admin.site.urls),
    
    # Root redirect
    path('', redirect_to_dashboard, name='root_redirect'),
    
    # Authentication URLs
    path('accounts/', include('app.accounts.urls')),
    
    # Social authentication - will handle /accounts/google/ etc.
    path('auth/', include('allauth.urls')),  # Changed from 'accounts/' to 'auth/'
    
    # Upload app URLs (your existing upload functionality)
    path('upload/', include('app.upload_app.urls')),
    
    # ETL app URLs (existing ETL processing functionality)
    path('etl/', include('app.etl_app.urls')),

    # Geospatial merger app URLs (NEW - FIXED PATH)
    path('geospatial/', include('app.geospatial_merger.urls')),
    
    # Built-in Django auth URLs
    # path('auth/', include('django.contrib.auth.urls')),
]

