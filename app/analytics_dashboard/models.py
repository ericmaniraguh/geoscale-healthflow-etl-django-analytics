"""
Geospatial Analytics Models - Integrated with Rwanda Location Hierarchy
Links to existing Province, District, Sector models in accounts app

app/analytics_dashboard/models.py
"""

from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.apps import apps


class HealthCenterLocation(models.Model):
    """
    Health Centre linked to existing Rwanda location hierarchy
    Contains geographic coordinates and terrain data
    """
    
    # Reference to existing location hierarchy
    sector = models.ForeignKey('accounts.Sector', on_delete=models.CASCADE, related_name='health_centres')
    
    # Health centre details
    code = models.CharField(max_length=50, unique=True)
    name = models.CharField(max_length=255)
    
    # Geographic coordinates
    latitude = models.FloatField(validators=[MinValueValidator(-90), MaxValueValidator(90)])
    longitude = models.FloatField(validators=[MinValueValidator(-180), MaxValueValidator(180)])
    
    # Elevation & slope data
    elevation_m = models.FloatField(null=True, blank=True, help_text="Elevation in meters")
    slope_percent = models.FloatField(null=True, blank=True, help_text="Slope percentage (0-100)")
    slope_degrees = models.FloatField(null=True, blank=True, help_text="Slope in degrees")
    
    # Geospatial geometry (GeoJSON)
    geojson_geometry = models.JSONField(null=True, blank=True, help_text="GeoJSON Point or Polygon")
    
    # Status
    is_active = models.BooleanField(default=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['sector', 'name']
        indexes = [
            models.Index(fields=['code']),
            models.Index(fields=['sector']),
            models.Index(fields=['is_active']),
        ]
        verbose_name = "Health Centre Location"
        verbose_name_plural = "Health Centre Locations"
    
    def __str__(self):
        return f"{self.name} ({self.sector.name})"
    
    def get_province(self):
        """Get province through district through sector"""
        return self.sector.district.province
    
    def get_district(self):
        """Get district through sector"""
        return self.sector.district
    
    @property
    def province_name(self):
        return self.get_province().name
    
    @property
    def district_name(self):
        return self.get_district().name
    
    @property
    def sector_name(self):
        return self.sector.name
    
    def get_geojson_point(self):
        """Return GeoJSON Point feature"""
        return {
            "type": "Point",
            "coordinates": [self.longitude, self.latitude]
        }


class MalariaAnalyticsAggregated(models.Model):
    """
    Pre-aggregated malaria surveillance data
    Aggregated by health centre and time period
    Updated daily by Airflow pipeline
    """
    
    # Reference to health centre
    health_centre = models.ForeignKey(HealthCenterLocation, on_delete=models.CASCADE, related_name='analytics')
    
    # Time dimensions
    year = models.IntegerField()
    month = models.IntegerField(null=True, blank=True)  # 1-12, null = yearly
    date = models.DateField(null=True, blank=True)
    
    # Case counts
    total_tests = models.IntegerField(default=0)
    positive_cases = models.IntegerField(default=0)
    negative_cases = models.IntegerField(default=0)
    
    # Calculated rates
    positivity_rate = models.FloatField(default=0.0)
    negativity_rate = models.FloatField(default=0.0)
    
    # Gender breakdown
    female_tests = models.IntegerField(default=0)
    female_positive = models.IntegerField(default=0)
    female_positivity_rate = models.FloatField(default=0.0)
    
    male_tests = models.IntegerField(default=0)
    male_positive = models.IntegerField(default=0)
    male_positivity_rate = models.FloatField(default=0.0)
    
    # Age group breakdown (optional)
    under_5_tests = models.IntegerField(default=0)
    under_5_positive = models.IntegerField(default=0)
    
    over_5_tests = models.IntegerField(default=0)
    over_5_positive = models.IntegerField(default=0)
    
    # Environmental factors
    temperature_avg = models.FloatField(null=True, blank=True)
    precipitation_mm = models.FloatField(null=True, blank=True)
    humidity_percent = models.FloatField(null=True, blank=True)
    
    # Data quality
    data_completeness = models.FloatField(default=100.0)
    is_provisional = models.BooleanField(default=False)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['-date', '-year', 'health_centre']
        unique_together = ('health_centre', 'year', 'month', 'date')
        indexes = [
            models.Index(fields=['year', 'month']),
            models.Index(fields=['health_centre', 'year']),
            models.Index(fields=['date']),
            models.Index(fields=['positivity_rate']),
        ]
    
    def __str__(self):
        period = f"{self.year}-{self.month:02d}" if self.month else str(self.year)
        return f"{self.health_centre.name} - {period}"
    
    def get_sector(self):
        return self.health_centre.sector
    
    def get_district(self):
        return self.health_centre.get_district()
    
    def get_province(self):
        return self.health_centre.get_province()


class LocationHierarchyCache(models.Model):
    """
    Cache for dynamic location hierarchy
    Used to speed up filter dropdowns
    Updated after location data changes
    """
    
    # Cached data as JSON
    data = models.JSONField()
    
    # Timestamp
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name_plural = "Location Hierarchy Caches"
    
    @classmethod
    def get_or_build(cls, force_rebuild=False):
        """Get cached hierarchy or build from database"""
        try:
            cache_obj = cls.objects.first()
            if cache_obj and not force_rebuild:
                return cache_obj.data
        except cls.DoesNotExist:
            pass
        
        # Build hierarchy from database
        hierarchy = cls._build_hierarchy()
        
        # Cache it
        cls.objects.all().delete()
        cache_obj = cls.objects.create(data=hierarchy)
        
        return hierarchy
    
    @staticmethod
    def _build_hierarchy():
        """Build location hierarchy from database"""
        try:
            Province = apps.get_model('accounts', 'Province')
            District = apps.get_model('accounts', 'District')
            Sector = apps.get_model('accounts', 'Sector')
            
            # Get all provinces
            provinces = list(Province.objects.values_list('name', flat=True).order_by('name'))
            
            # Get districts by province
            districts = {}
            for province in provinces:
                prov_obj = Province.objects.get(name=province)
                dists = list(
                    District.objects.filter(province=prov_obj)
                    .values_list('name', flat=True)
                    .order_by('name')
                )
                districts[province] = dists
            
            # Get sectors by district
            sectors = {}
            for province in provinces:
                prov_obj = Province.objects.get(name=province)
                for dist_name in districts.get(province, []):
                    dist_obj = District.objects.get(province=prov_obj, name=dist_name)
                    sects = list(
                        Sector.objects.filter(district=dist_obj)
                        .values_list('name', flat=True)
                        .order_by('name')
                    )
                    sectors[dist_name] = sects
            
            return {
                'provinces': provinces,
                'districts': districts,
                'sectors': sectors,
            }
        
        except Exception as e:
            print(f"Error building hierarchy: {e}")
            return {
                'provinces': [],
                'districts': {},
                'sectors': {},
            }


class DashboardCache(models.Model):
    """
    Cache for expensive analytics queries
    Cleared by Airflow after data pipeline runs
    """
    
    cache_key = models.CharField(max_length=255, unique=True, db_index=True)
    cache_data = models.JSONField()
    expires_at = models.DateTimeField(db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        indexes = [
            models.Index(fields=['cache_key', 'expires_at']),
        ]
        verbose_name_plural = "Dashboard Caches"
    
    def is_expired(self):
        """Check if cache has expired"""
        from django.utils import timezone
        return timezone.now() > self.expires_at
    
    @classmethod
    def get_or_fetch(cls, key, fetch_func, ttl=600):
        """Get cached data or fetch new"""
        try:
            cache_obj = cls.objects.get(cache_key=key)
            if not cache_obj.is_expired():
                return cache_obj.cache_data, True  # (data, is_cached)
            else:
                cache_obj.delete()
        except cls.DoesNotExist:
            pass
        
        # Fetch new data
        data = fetch_func()
        
        # Cache it
        from django.utils import timezone
        from datetime import timedelta
        
        cls.objects.update_or_create(
            cache_key=key,
            defaults={
                'cache_data': data,
                'expires_at': timezone.now() + timedelta(seconds=ttl)
            }
        )
        
        return data, False  # (data, is_cached)


__all__ = [
    'HealthCenterLocation',
    'MalariaAnalyticsAggregated',
    'LocationHierarchyCache',
    'DashboardCache',
]