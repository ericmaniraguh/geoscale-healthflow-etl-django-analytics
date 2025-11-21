"""
Management command to load sample analytics data for testing dashboard

This creates:
1. Sample health centres with geospatial coordinates
2. Sample malaria analytics data for 2021-2023

Usage:
    python manage.py load_sample_analytics_data
    python manage.py load_sample_analytics_data --clear  # Clear existing data first
"""

from django.core.management.base import BaseCommand
from app.accounts.models import Province, District, Sector
from app.analytics_dashboard.models import HealthCenterLocation, MalariaAnalyticsAggregated
from datetime import date
import random


class Command(BaseCommand):
    help = 'Load sample analytics data for dashboard testing'

    def add_arguments(self, parser):
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Clear existing analytics data before loading',
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.WARNING('Loading sample analytics data...'))

        # Check if locations exist
        if Province.objects.count() == 0:
            self.stdout.write(self.style.ERROR('⚠ No location data found!'))
            self.stdout.write(self.style.WARNING('Please run: python manage.py load_rwanda_locations'))
            return

        # Clear existing data if requested
        if options['clear']:
            self.stdout.write('Clearing existing analytics data...')
            MalariaAnalyticsAggregated.objects.all().delete()
            HealthCenterLocation.objects.all().delete()
            self.stdout.write(self.style.SUCCESS('✓ Cleared existing data'))

        # Sample health centres data
        sample_health_centres = [
            # Eastern Province - Bugesera District
            {'province': 'Eastern Province', 'district': 'Bugesera', 'sector': 'Nyamata', 'name': 'Nyamata Health Center', 'code': 'HC_NYA_001', 'lat': -2.145, 'lon': 30.081},
            {'province': 'Eastern Province', 'district': 'Bugesera', 'sector': 'Gashora', 'name': 'Gashora Health Center', 'code': 'HC_GAS_001', 'lat': -2.182, 'lon': 30.193},
            {'province': 'Eastern Province', 'district': 'Bugesera', 'sector': 'Rilima', 'name': 'Rilima Health Center', 'code': 'HC_RIL_001', 'lat': -2.165, 'lon': 30.135},

            # Kigali City - Gasabo District
            {'province': 'Kigali City', 'district': 'Gasabo', 'sector': 'Remera', 'name': 'Remera Health Center', 'code': 'HC_REM_001', 'lat': -1.957, 'lon': 30.089},
            {'province': 'Kigali City', 'district': 'Gasabo', 'sector': 'Kimironko', 'name': 'Kimironko Health Center', 'code': 'HC_KIM_001', 'lat': -1.943, 'lon': 30.126},
            {'province': 'Kigali City', 'district': 'Gasabo', 'sector': 'Kacyiru', 'name': 'Kacyiru Health Center', 'code': 'HC_KAC_001', 'lat': -1.942, 'lon': 30.070},

            # Southern Province - Huye District
            {'province': 'Southern Province', 'district': 'Huye', 'sector': 'Huye', 'name': 'Huye Health Center', 'code': 'HC_HUY_001', 'lat': -2.595, 'lon': 29.739},
            {'province': 'Southern Province', 'district': 'Huye', 'sector': 'Tumba', 'name': 'Tumba Health Center', 'code': 'HC_TUM_001', 'lat': -2.587, 'lon': 29.741},

            # Northern Province - Musanze District
            {'province': 'Northern Province', 'district': 'Musanze', 'sector': 'Muhoza', 'name': 'Muhoza Health Center', 'code': 'HC_MUH_001', 'lat': -1.502, 'lon': 29.633},
            {'province': 'Northern Province', 'district': 'Musanze', 'sector': 'Cyuve', 'name': 'Cyuve Health Center', 'code': 'HC_CYU_001', 'lat': -1.548, 'lon': 29.668},

            # Western Province - Rubavu District
            {'province': 'Western Province', 'district': 'Rubavu', 'sector': 'Gisenyi', 'name': 'Gisenyi Health Center', 'code': 'HC_GIS_001', 'lat': -1.704, 'lon': 29.256},
            {'province': 'Western Province', 'district': 'Rubavu', 'sector': 'Rubavu', 'name': 'Rubavu Health Center', 'code': 'HC_RUB_001', 'lat': -1.678, 'lon': 29.279},
        ]

        # Create health centres
        health_centres_created = 0
        health_centres_objects = {}

        for hc_data in sample_health_centres:
            try:
                # Get location hierarchy
                province = Province.objects.get(name=hc_data['province'])
                district = District.objects.get(province=province, name=hc_data['district'])
                sector = Sector.objects.get(district=district, name=hc_data['sector'])

                # Create health centre
                hc, created = HealthCenterLocation.objects.get_or_create(
                    code=hc_data['code'],
                    defaults={
                        'sector': sector,
                        'name': hc_data['name'],
                        'latitude': hc_data['lat'],
                        'longitude': hc_data['lon'],
                        'elevation_m': random.uniform(1200, 2000),
                        'slope_percent': random.uniform(0, 30),
                        'slope_degrees': random.uniform(0, 17),
                        'is_active': True
                    }
                )

                if created:
                    health_centres_created += 1
                    self.stdout.write(f'  ✓ Created: {hc_data["name"]}')

                health_centres_objects[hc_data['code']] = hc

            except (Province.DoesNotExist, District.DoesNotExist, Sector.DoesNotExist) as e:
                self.stdout.write(self.style.WARNING(f'  ⚠ Skipped {hc_data["name"]}: {str(e)}'))

        self.stdout.write(self.style.SUCCESS(f'\n✓ Created {health_centres_created} health centres'))

        # Generate analytics data for 2021-2023
        self.stdout.write('\nGenerating analytics data for 2021-2023...')
        analytics_created = 0

        years = [2021, 2022, 2023]
        months = list(range(1, 13))

        for hc_code, hc in health_centres_objects.items():
            for year in years:
                for month in months:
                    # Generate realistic malaria data
                    # Higher rates in rainy seasons (March-May, October-November)
                    is_rainy_season = month in [3, 4, 5, 10, 11]
                    base_tests = random.randint(200, 500)

                    if is_rainy_season:
                        total_tests = base_tests + random.randint(100, 200)
                        positivity_multiplier = random.uniform(1.3, 1.8)
                    else:
                        total_tests = base_tests
                        positivity_multiplier = random.uniform(0.7, 1.2)

                    # Calculate cases
                    base_positivity = random.uniform(15, 35)  # 15-35% positivity rate
                    positivity_rate = min(base_positivity * positivity_multiplier, 95)
                    positive_cases = int(total_tests * (positivity_rate / 100))
                    negative_cases = total_tests - positive_cases

                    # Gender breakdown (roughly 50/50)
                    female_tests = int(total_tests * random.uniform(0.48, 0.52))
                    male_tests = total_tests - female_tests

                    female_positive = int(positive_cases * random.uniform(0.48, 0.52))
                    male_positive = positive_cases - female_positive

                    # Age breakdown
                    under_5_tests = int(total_tests * random.uniform(0.3, 0.4))
                    over_5_tests = total_tests - under_5_tests

                    under_5_positive = int(positive_cases * random.uniform(0.35, 0.45))
                    over_5_positive = positive_cases - under_5_positive

                    # Environmental data
                    # Temperature varies by season
                    if month in [6, 7, 8]:  # Cooler dry season
                        temp = random.uniform(18, 22)
                    else:
                        temp = random.uniform(20, 25)

                    # Precipitation varies by season
                    if is_rainy_season:
                        precip = random.uniform(100, 200)
                    else:
                        precip = random.uniform(10, 60)

                    # Create analytics record
                    analytics, created = MalariaAnalyticsAggregated.objects.get_or_create(
                        health_centre=hc,
                        year=year,
                        month=month,
                        date=date(year, month, 15),  # Mid-month date
                        defaults={
                            'total_tests': total_tests,
                            'positive_cases': positive_cases,
                            'negative_cases': negative_cases,
                            'positivity_rate': round(positivity_rate, 2),
                            'negativity_rate': round(100 - positivity_rate, 2),
                            'female_tests': female_tests,
                            'female_positive': female_positive,
                            'female_positivity_rate': round((female_positive / female_tests * 100) if female_tests > 0 else 0, 2),
                            'male_tests': male_tests,
                            'male_positive': male_positive,
                            'male_positivity_rate': round((male_positive / male_tests * 100) if male_tests > 0 else 0, 2),
                            'under_5_tests': under_5_tests,
                            'under_5_positive': under_5_positive,
                            'over_5_tests': over_5_tests,
                            'over_5_positive': over_5_positive,
                            'temperature_avg': round(temp, 1),
                            'precipitation_mm': round(precip, 1),
                            'humidity_percent': random.uniform(60, 85),
                            'data_completeness': 100.0,
                            'is_provisional': False
                        }
                    )

                    if created:
                        analytics_created += 1

        self.stdout.write(self.style.SUCCESS(f'✓ Created {analytics_created} analytics records'))

        # Summary
        self.stdout.write(self.style.SUCCESS('\n=== Summary ==='))
        self.stdout.write(self.style.SUCCESS(f'Health Centres: {HealthCenterLocation.objects.count()}'))
        self.stdout.write(self.style.SUCCESS(f'Analytics Records: {MalariaAnalyticsAggregated.objects.count()}'))
        self.stdout.write(self.style.SUCCESS(f'Years covered: {", ".join(map(str, years))}'))
        self.stdout.write(self.style.SUCCESS('\n✓ Sample analytics data loaded successfully!'))
        self.stdout.write(self.style.WARNING('\nYou can now access the analytics dashboard at /analytics/'))
