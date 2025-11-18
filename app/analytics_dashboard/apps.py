from django.apps import AppConfig


class AnalyticsDashboardConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'app.analytics_dashboard'
    verbose_name = 'Analytics Dashboard'  # FIXED: Was "Analytocs Dashboard"
    
    def ready(self):
        """Initialize app when Django starts"""
        pass