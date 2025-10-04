# Django ETL Project - Malaria API Calculator ETL View
# This view handles the ETL process for malaria API calculations, including dynamic table naming,

from django.http import JsonResponse
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.conf import settings
import json
import logging
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text
import re
import uuid

logger = logging.getLogger(__name__)

@method_decorator(csrf_exempt, name='dispatch')
class MalariaAPICalculatorView(View):
    """
    Enhanced Malaria API Calculator with Province/District Selection, Unique IDs, and Smart Updates
    
    Features:
    - Province and District selection
    - API calculation per sector within selected district
    - Custom table naming: API_districtname_years
    - Multiple years selection
    - PostgreSQL saving with dynamic table names
    - Auto-generated unique IDs for all records
    - Smart update handling (replace/append modes)
    - Formatted timestamps (YYYY-MM-DD HH:MM)
    """
    
    def __init__(self):
        super().__init__()
        
        # Use Django settings (your working MongoDB connection)
        self.mongo_uri = getattr(settings, 'MONGO_URI')
        self.mongo_db = getattr(settings, 'MONGO_HMIS_DB', 'hmis_records_db')
        
        # PostgreSQL config from Django settings
        db_config = settings.DATABASES['default']
        self.pg_config = {
            'host': db_config['HOST'],
            'port': db_config['PORT'],
            'database': db_config['NAME'],
            'user': db_config['USER'],
            'password': db_config['PASSWORD']
        }
        
        print(f"Enhanced Malaria API Calculator initialized successfully")
        print(f"Ready to calculate API by Province/District/Sector")
    
    def _format_timestamp(self, dt):
        """Format timestamp to YYYY-MM-DD HH:MM format"""
        return dt.strftime('%Y-%m-%d %H:%M')
    
    def _generate_unique_id(self):
        """Generate a unique ID for each record"""
        return str(uuid.uuid4())
    
    def _sanitize_table_name_part(self, name):
        """Sanitize a string to be used as part of a table name"""
        if not name:
            return "unknown"
        
        # Convert to string and lowercase
        sanitized = str(name).lower()
        
        # Replace spaces and special characters with underscores
        sanitized = re.sub(r'[^a-z0-9_]', '_', sanitized)
        
        # Remove consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        
        # Limit length
        if len(sanitized) > 15:
            sanitized = sanitized[:15]
        
        return sanitized if sanitized else "unknown"
    
    def _generate_api_table_name(self, district, province, years):
        """Generate table name: API_provincename_districtname_years"""
        district_clean = self._sanitize_table_name_part(district)
        province_clean = self._sanitize_table_name_part(province)
        
        if isinstance(years, list) and len(years) > 0:
            sorted_years = sorted(years)
            if len(sorted_years) <= 3:
                year_part = '_'.join(map(str, sorted_years))
            else:
                # Use range format for many years
                year_part = f"{min(sorted_years)}_{max(sorted_years)}"
        else:
            year_part = "all"
        
        table_name = f"hc_api_{province_clean}_{district_clean}"
        
        # Ensure table name is not too long (PostgreSQL limit is 63 characters)
        if len(table_name) > 60:
            # Truncate names if needed while keeping important parts
            excess = len(table_name) - 60
            
            # Strategy: truncate province and district proportionally
            available_length = 60 - len(f"api__{year_part}") - 2  # 2 underscores
            province_max = min(len(province_clean), available_length // 2)
            district_max = available_length - province_max
            
            province_short = province_clean[:province_max] if province_max > 0 else province_clean[:6]
            district_short = district_clean[:district_max] if district_max > 0 else district_clean[:6]
            
            table_name = f"hc_api_{province_short}_{district_short}_{year_part}"
        
        return table_name
    
    def _connect_mongodb(self):
        """Connect to MongoDB"""
        try:
            client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=30000)
            client.admin.command('ismaster')
            return client
        except Exception as e:
            logger.error(f"MongoDB connection failed: {str(e)}")
            return None
    
    def _discover_data_collections(self, client):
        """Discover available data collections and metadata"""
        try:
            db = client[self.mongo_db]
            all_collections = db.list_collection_names()
            
            # Look for metadata collection
            metadata_collections = [col for col in all_collections if 'metadata' in col.lower()]
            data_collections = []
            datasets_info = []
            
            # If we have metadata collections, check them
            for meta_col_name in metadata_collections:
                meta_collection = db[meta_col_name]
                metadata_docs = list(meta_collection.find({}))
                
                for doc in metadata_docs:
                    collection_name = doc.get('collection_name')
                    if collection_name and collection_name in all_collections:
                        # Extract years from columns
                        years = []
                        columns = doc.get('columns', [])
                        for col in columns:
                            if 'Total Cases_' in col:
                                year_match = re.search(r'Total Cases_(\d{4})', col)
                                if year_match:
                                    years.append(int(year_match.group(1)))
                        
                        dataset_info = {
                            'collection_name': collection_name,
                            'dataset_name': doc.get('dataset_name', 'Unknown Dataset'),
                            'district': doc.get('district', 'Unknown'),
                            'available_years': sorted(list(set(years))),
                            'records_count': doc.get('records_count', 0),
                            'description': doc.get('description', ''),
                            'upload_date': doc.get('upload_date')
                        }
                        datasets_info.append(dataset_info)
                        data_collections.append(collection_name)
            
            # If no metadata found, check collections directly
            if not datasets_info:
                for col_name in all_collections:
                    if col_name != 'metadata' and not col_name.endswith('_metadata'):
                        collection = db[col_name]
                        sample = collection.find_one()
                        
                        if sample and any('Total Cases_' in key for key in sample.keys()):
                            # This looks like malaria data
                            years = []
                            for key in sample.keys():
                                if 'Total Cases_' in key:
                                    year_match = re.search(r'Total Cases_(\d{4})', key)
                                    if year_match:
                                        years.append(int(year_match.group(1)))
                            
                            dataset_info = {
                                'collection_name': col_name,
                                'dataset_name': f'Malaria Data from {col_name}',
                                'district': sample.get('District', 'Multiple'),
                                'available_years': sorted(list(set(years))),
                                'records_count': collection.count_documents({}),
                                'description': 'Discovered malaria dataset',
                                'upload_date': None
                            }
                            datasets_info.append(dataset_info)
                            data_collections.append(col_name)
            
            return {
                'available_collections': data_collections,
                'datasets': datasets_info,
                'total_datasets': len(datasets_info),
                'all_years': sorted(list(set(year for dataset in datasets_info for year in dataset['available_years']))),
                'all_districts': sorted(list(set(dataset['district'] for dataset in datasets_info if dataset['district'] != 'Unknown')))
            }
            
        except Exception as e:
            logger.error(f"Error discovering collections: {str(e)}")
            return {'error': str(e)}
    
    def _get_location_hierarchy(self, client, collection_names):
        """Get hierarchical location data: provinces -> districts -> sectors"""
        try:
            location_hierarchy = {}
            all_provinces = set()
            all_districts = set()
            
            db = client[self.mongo_db]
            
            for col_name in collection_names:
                collection = db[col_name]
                
                # Get all location combinations
                pipeline = [
                    {"$group": {
                        "_id": {
                            "province": "$Province",
                            "district": "$District", 
                            "sector": "$Sector"
                        }
                    }},
                    {"$match": {"_id.province": {"$ne": None, "$ne": ""}}},
                    {"$sort": {"_id.province": 1, "_id.district": 1, "_id.sector": 1}}
                ]
                
                results = list(collection.aggregate(pipeline))
                
                for result in results:
                    loc = result['_id']
                    province = loc.get('province', '').strip()
                    district = loc.get('district', '').strip()
                    sector = loc.get('sector', '').strip()
                    
                    if province and district:
                        all_provinces.add(province)
                        all_districts.add(district)
                        
                        if province not in location_hierarchy:
                            location_hierarchy[province] = {}
                        
                        if district not in location_hierarchy[province]:
                            location_hierarchy[province][district] = []
                        
                        if sector and sector not in location_hierarchy[province][district]:
                            location_hierarchy[province][district].append(sector)
            
            # Sort sectors for each district
            for province in location_hierarchy:
                for district in location_hierarchy[province]:
                    location_hierarchy[province][district].sort()
            
            return {
                'hierarchy': location_hierarchy,
                'provinces': sorted(list(all_provinces)),
                'districts': sorted(list(all_districts))
            }
            
        except Exception as e:
            logger.error(f"Error getting location hierarchy: {str(e)}")
            return {'hierarchy': {}, 'provinces': [], 'districts': []}
    
    def _calculate_api_by_sector(self, client, collection_names, province, district, years):
        """Calculate API for each sector in the selected district with unique IDs"""
        try:
            all_results = []
            db = client[self.mongo_db]
            
            for col_name in collection_names:
                collection = db[col_name]
                
                # Build query for specific province and district
                query = {
                    'Province': {'$regex': f'^{re.escape(province)}$', '$options': 'i'},
                    'District': {'$regex': f'^{re.escape(district)}$', '$options': 'i'}
                }
                
                # Get documents for this province/district
                documents = list(collection.find(query))
                if not documents:
                    continue
                
                # Process each sector (document) and year combination
                for doc in documents:
                    sector = doc.get('Sector', '').strip()
                    if not sector:
                        continue
                    
                    for year in years:
                        cases_col = f'Total Cases_{year}'
                        pop_col = f'Pop{year}'
                        incidence_col = f'Incidence_{year}'
                        
                        # Skip if required columns don't exist
                        if cases_col not in doc or pop_col not in doc:
                            continue
                        
                        # Extract and validate data
                        try:
                            total_cases = float(doc.get(cases_col, 0) or 0)
                            population = float(doc.get(pop_col, 0) or 0)
                            original_incidence = float(doc.get(incidence_col, 0) or 0)
                        except (ValueError, TypeError):
                            continue
                        
                        # Calculate API
                        api = (total_cases / population * 1000) if population > 0 else 0
                        
                        # WHO Risk Categories
                        if api == 0:
                            risk_category = 'No Transmission'
                        elif api < 1:
                            risk_category = 'Very Low Risk'
                        elif api < 5:
                            risk_category = 'Low Risk'
                        elif api < 50:
                            risk_category = 'Moderate Risk'
                        elif api < 100:
                            risk_category = 'High Risk'
                        else:
                            risk_category = 'Very High Risk'
                        
                        result = {
                            'unique_id': self._generate_unique_id(),  # Add unique ID
                            'province': province,
                            'district': district,
                            'sector': sector,
                            'year': year,
                            'total_cases': int(total_cases),
                            'population': int(population),
                            'api': round(api, 2),
                            'risk_category': risk_category,
                            'incidence_original': round(original_incidence, 2),
                            'cases_per_1000': round(api, 2),
                            'high_burden': api >= 50,
                            'elimination_target': api < 1,
                            'source_collection': col_name,
                            'created_at': self._format_timestamp(datetime.now()),  # Formatted timestamp
                            'updated_at': self._format_timestamp(datetime.now())
                        }
                        
                        all_results.append(result)
            
            return all_results
            
        except Exception as e:
            logger.error(f"Error calculating API by sector: {str(e)}")
            return []
    
     # 
    def _save_to_postgres(self, data, table_name, update_mode='replace'):
        """Save results to PostgreSQL with dynamic table name and smart update handling - FIXED"""
        try:
            if not data:
                return False, "No data to save"
            
            # Create connection
            engine = create_engine(
                f"postgresql://{self.pg_config['user']}:{self.pg_config['password']}@"
                f"{self.pg_config['host']}:{self.pg_config['port']}/{self.pg_config['database']}"
            )
            
            # KEY FIX: Use engine.begin() instead of engine.connect()
            with engine.begin() as conn:  # This automatically handles transactions
                
                # Handle table creation based on update mode
                if update_mode == 'replace':
                    drop_table_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
                    conn.execute(text(drop_table_sql))
                    # No conn.commit() needed - begin() handles this automatically
                
                # Create table with proper structure including unique ID
                create_table_sql = f"""
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    unique_id VARCHAR(36) UNIQUE NOT NULL,
                    province VARCHAR(100),
                    district VARCHAR(100),
                    sector VARCHAR(100),
                    year INTEGER,
                    total_cases INTEGER,
                    population INTEGER,
                    api DECIMAL(10, 2),
                    risk_category VARCHAR(50),
                    incidence_original DECIMAL(10, 2),
                    cases_per_1000 DECIMAL(10, 2),
                    high_burden BOOLEAN,
                    elimination_target BOOLEAN,
                    source_collection VARCHAR(200),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                """
                
                # Create indexes separately
                create_indexes_sql = f"""
                CREATE INDEX idx_{table_name.replace('-', '_')[:50]}_unique_id ON {table_name}(unique_id);
                CREATE INDEX idx_{table_name.replace('-', '_')[:50]}_sector_year ON {table_name}(sector, year);
                CREATE INDEX idx_{table_name.replace('-', '_')[:50]}_api ON {table_name}(api);
                CREATE INDEX idx_{table_name.replace('-', '_')[:50]}_risk ON {table_name}(risk_category);
                CREATE INDEX idx_{table_name.replace('-', '_')[:50]}_location ON {table_name}(province, district, sector);
                """
                
                # Execute table creation and indexes
                conn.execute(text(create_table_sql))
                conn.execute(text(create_indexes_sql))
                # No conn.commit() needed - begin() handles this automatically
                
                # Insert data with smart update handling
                records_inserted = 0
                records_updated = 0
                
                for record in data:
                    try:
                        insert_data = {
                            'unique_id': record.get('unique_id', self._generate_unique_id()),
                            'province': record.get('province', ''),
                            'district': record.get('district', ''),
                            'sector': record.get('sector', ''),
                            'year': record.get('year'),
                            'total_cases': record.get('total_cases', 0),
                            'population': record.get('population', 0),
                            'api': record.get('api', 0.0),
                            'risk_category': record.get('risk_category', ''),
                            'incidence_original': record.get('incidence_original', 0.0),
                            'cases_per_1000': record.get('cases_per_1000', 0.0),
                            'high_burden': record.get('high_burden', False),
                            'elimination_target': record.get('elimination_target', False),
                            'source_collection': record.get('source_collection', ''),
                            'created_at': record.get('created_at', self._format_timestamp(datetime.now())),
                            'updated_at': record.get('updated_at', self._format_timestamp(datetime.now()))
                        }
                        
                        if update_mode == 'append':
                            # Use INSERT ... ON CONFLICT for upsert behavior
                            upsert_sql = f"""
                            INSERT INTO {table_name} 
                            (unique_id, province, district, sector, year, total_cases, population, api,
                            risk_category, incidence_original, cases_per_1000, high_burden, elimination_target,
                            source_collection, created_at, updated_at)
                            VALUES (:unique_id, :province, :district, :sector, :year, :total_cases, :population, :api,
                                    :risk_category, :incidence_original, :cases_per_1000, :high_burden, :elimination_target,
                                    :source_collection, :created_at, :updated_at)
                            ON CONFLICT (unique_id) DO UPDATE SET
                                province = EXCLUDED.province,
                                district = EXCLUDED.district,
                                sector = EXCLUDED.sector,
                                year = EXCLUDED.year,
                                total_cases = EXCLUDED.total_cases,
                                population = EXCLUDED.population,
                                api = EXCLUDED.api,
                                risk_category = EXCLUDED.risk_category,
                                incidence_original = EXCLUDED.incidence_original,
                                cases_per_1000 = EXCLUDED.cases_per_1000,
                                high_burden = EXCLUDED.high_burden,
                                elimination_target = EXCLUDED.elimination_target,
                                source_collection = EXCLUDED.source_collection,
                                created_at = EXCLUDED.created_at,
                                updated_at = NOW()
                            """
                            
                            result_proxy = conn.execute(text(upsert_sql), insert_data)
                            if result_proxy.rowcount == 1:
                                records_inserted += 1
                            else:
                                records_updated += 1
                        else:
                            # Simple insert for replace mode
                            insert_sql = f"""
                            INSERT INTO {table_name} 
                            (unique_id, province, district, sector, year, total_cases, population, api,
                            risk_category, incidence_original, cases_per_1000, high_burden, elimination_target,
                            source_collection, created_at, updated_at)
                            VALUES (:unique_id, :province, :district, :sector, :year, :total_cases, :population, :api,
                                    :risk_category, :incidence_original, :cases_per_1000, :high_burden, :elimination_target,
                                    :source_collection, :created_at, :updated_at)
                            """
                            
                            conn.execute(text(insert_sql), insert_data)
                            records_inserted += 1
                        
                    except Exception as record_error:
                        logger.error(f"Error inserting/updating record: {str(record_error)}")
                        continue
                
                # Verify the save worked
                verify_sql = f"SELECT COUNT(*) FROM {table_name}"
                result = conn.execute(text(verify_sql))
                final_count = result.fetchone()[0]
                
                logger.info(f"MALARIA API: Successfully saved {final_count} records to {table_name}")
                
                # The transaction will automatically commit when exiting the 'with' block
                
                message = f"Successfully processed {len(data)} records to table '{table_name}'. Final count: {final_count}"
                if update_mode == 'append':
                    message += f" (inserted: {records_inserted}, updated: {records_updated})"
                
                return True, message
            
        except Exception as e:
            error_msg = f"Error saving to PostgreSQL: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def get(self, request):
        """Handle GET requests"""
        start_time = datetime.now()
        
        try:
            # Get parameters
            province = request.GET.get('province', '').strip()
            district = request.GET.get('district', '').strip()
            years_param = request.GET.get('years', '')
            show_available = request.GET.get('show_available', 'false').lower() == 'true'
            save_to_postgres = request.GET.get('save_to_postgres', 'true').lower() == 'true'
            update_mode = request.GET.get('update_mode', 'replace').lower()  # Add update mode support
            
            # Validate update mode
            if update_mode not in ['replace', 'append']:
                return JsonResponse({
                    'success': False,
                    'error': 'update_mode must be either "replace" or "append"',
                    'received': update_mode,
                    'timestamp': self._format_timestamp(datetime.now())
                }, status=400)
            
            # Connect to MongoDB
            client = self._connect_mongodb()
            if not client:
                return JsonResponse({
                    'success': False,
                    'error': 'Cannot connect to MongoDB. Please check your connection.',
                    'timestamp': self._format_timestamp(datetime.now())
                }, status=503)
            
            try:
                # Discover available data
                discovery = self._discover_data_collections(client)
                
                if 'error' in discovery:
                    return JsonResponse({
                        'success': False,
                        'error': f'Error accessing data: {discovery["error"]}',
                        'timestamp': self._format_timestamp(datetime.now())
                    }, status=500)
                
                if not discovery['datasets']:
                    return JsonResponse({
                        'success': False,
                        'error': 'No malaria datasets found in the database',
                        'suggestion': 'Please upload malaria data first',
                        'timestamp': self._format_timestamp(datetime.now())
                    }, status=404)
                
                # Get location hierarchy
                location_data = self._get_location_hierarchy(client, discovery['available_collections'])
                
                # If user wants to see available options
                if show_available:
                    return JsonResponse({
                        'success': True,
                        'message': 'Available options for API calculation by Province/District/Sector',
                        'available_options': {
                            'years': discovery['all_years'],
                            'provinces': location_data['provinces'],
                            'districts': location_data['districts'],
                            'location_hierarchy': location_data['hierarchy']
                        },
                        'datasets': discovery['datasets'],
                        'usage_examples': {
                            'by_province_district': '?province=Southern&district=Huye&years=2023&save_to_postgres=true&update_mode=replace',
                            'multiple_years': '?province=Southern&district=Huye&years=2021,2022,2023&save_to_postgres=true&update_mode=append',
                            'year_range': '?province=Kigali&district=Gasabo&years=2021-2023&save_to_postgres=true&update_mode=replace',
                            'table_naming': 'Results saved as: api_southern_huye_2023 or api_kigali_gasabo_2021_2022_2023'
                        },
                        'update_modes': {
                            'replace': 'Drops existing table and creates fresh data (default)',
                            'append': 'Uses INSERT ... ON CONFLICT to update existing records or insert new ones'
                        },
                        'features': [
                            'Auto-generated unique IDs for all records',
                            'Smart update handling (replace/append modes)',
                            'Formatted timestamps (YYYY-MM-DD HH:MM)',
                            'Dynamic table naming based on location and years',
                            'WHO risk category classification',
                            'PostgreSQL integration with comprehensive indexing'
                        ],
                        'note': 'API will be calculated for each sector within the selected district',
                        'timestamp': self._format_timestamp(datetime.now())
                    })
                
                # Validate required parameters
                if not province:
                    return JsonResponse({
                        'success': False,
                        'error': 'Province parameter is required',
                        'available_provinces': location_data['provinces'],
                        'suggestion': 'Use ?show_available=true to see all options',
                        'timestamp': self._format_timestamp(datetime.now())
                    }, status=400)
                
                if not district:
                    available_districts = []
                    if province in location_data['hierarchy']:
                        available_districts = list(location_data['hierarchy'][province].keys())
                    
                    return JsonResponse({
                        'success': False,
                        'error': 'District parameter is required',
                        'available_districts_in_province': available_districts,
                        'suggestion': f'Select a district from {province} province',
                        'timestamp': self._format_timestamp(datetime.now())
                    }, status=400)
                
                # Validate province and district combination
                if province not in location_data['hierarchy']:
                    return JsonResponse({
                        'success': False,
                        'error': f'Province "{province}" not found',
                        'available_provinces': location_data['provinces'],
                        'timestamp': self._format_timestamp(datetime.now())
                    }, status=400)
                
                if district not in location_data['hierarchy'][province]:
                    return JsonResponse({
                        'success': False,
                        'error': f'District "{district}" not found in province "{province}"',
                        'available_districts': list(location_data['hierarchy'][province].keys()),
                        'timestamp': self._format_timestamp(datetime.now())
                    }, status=400)
                
                # Parse years parameter
                years = []
                if years_param:
                    if years_param.lower() == 'all':
                        years = discovery['all_years']
                    elif '-' in years_param and ',' not in years_param:
                        # Range format: 2021-2023
                        try:
                            start_year, end_year = map(int, years_param.split('-'))
                            years = list(range(start_year, end_year + 1))
                        except ValueError:
                            return JsonResponse({
                                'success': False,
                                'error': f'Invalid year range format: {years_param}. Use format like 2021-2023',
                                'timestamp': self._format_timestamp(datetime.now())
                            }, status=400)
                    else:
                        # Comma-separated: 2021,2022,2023
                        try:
                            years = [int(y.strip()) for y in years_param.split(',')]
                        except ValueError:
                            return JsonResponse({
                                'success': False,
                                'error': f'Invalid year format: {years_param}. Use comma-separated years like 2021,2022,2023',
                                'timestamp': self._format_timestamp(datetime.now())
                            }, status=400)
                else:
                    # Default: use all available years
                    years = discovery['all_years']
                
                # Validate years
                available_years = set(discovery['all_years'])
                requested_years = set(years)
                invalid_years = requested_years - available_years
                
                if invalid_years:
                    return JsonResponse({
                        'success': False,
                        'error': f'Years {sorted(list(invalid_years))} are not available',
                        'available_years': discovery['all_years'],
                        'timestamp': self._format_timestamp(datetime.now())
                    }, status=400)
                
                # Calculate API by sector
                print(f"Calculating API for {province} > {district}, years: {years}, update_mode: {update_mode}")
                results = self._calculate_api_by_sector(
                    client, 
                    discovery['available_collections'], 
                    province,
                    district,
                    years
                )
                
                if not results:
                    return JsonResponse({
                        'success': False,
                        'message': 'No data found for the specified province/district/years combination',
                        'filters_applied': {
                            'province': province,
                            'district': district,
                            'years': years,
                            'update_mode': update_mode
                        },
                        'available_sectors': location_data['hierarchy'].get(province, {}).get(district, []),
                        'timestamp': self._format_timestamp(datetime.now())
                    })
                
                # Generate table name and save to PostgreSQL
                postgres_saved = False
                postgres_message = ""
                table_name = ""
                
                if save_to_postgres:
                    table_name = self._generate_api_table_name(district, province, years)
                    print(f"Saving to PostgreSQL table: {table_name} (mode: {update_mode})")
                    postgres_saved, postgres_message = self._save_to_postgres(results, table_name, update_mode)
                
                # Calculate summary statistics
                total_cases = sum(r['total_cases'] for r in results)
                total_population = sum(r['population'] for r in results)
                apis = [r['api'] for r in results if r['api'] > 0]
                sectors_processed = sorted(list(set(r['sector'] for r in results)))
                
                processing_time = (datetime.now() - start_time).total_seconds()
                
                return JsonResponse({
                    'success': True,
                    'message': f'Successfully calculated API for {len(results)} sector-year combinations in {district} district',
                    'formula': 'API = (Total Cases ÷ Population) × 1,000',
                    'results': results,
                    'summary': {
                        'province': province,
                        'district': district,
                        'sectors_processed': sectors_processed,
                        'years_processed': sorted(list(set(r['year'] for r in results))),
                        'total_records': len(results),
                        'total_cases': total_cases,
                        'total_population': total_population,
                        'overall_district_api': round((total_cases / total_population * 1000), 2) if total_population > 0 else 0,
                        'average_api': round(sum(apis) / len(apis), 2) if apis else 0,
                        'highest_api': max(apis) if apis else 0,
                        'lowest_api': min(apis) if apis else 0,
                        'high_burden_sectors': sum(1 for r in results if r['high_burden']),
                        'elimination_candidate_sectors': sum(1 for r in results if r['elimination_target']),
                        'risk_distribution': {
                            category: sum(1 for r in results if r['risk_category'] == category)
                            for category in ['No Transmission', 'Very Low Risk', 'Low Risk', 'Moderate Risk', 'High Risk', 'Very High Risk']
                        }
                    },
                    'filters_applied': {
                        'province': province,
                        'district': district,
                        'years': years,
                        'update_mode': update_mode
                    },
                    'postgres': {
                        'saved': postgres_saved,
                        'table_name': table_name if postgres_saved else None,
                        'message': postgres_message,
                        'update_mode': update_mode
                    },
                    'features_used': [
                        'Auto-generated unique IDs',
                        f'Update mode: {update_mode}',
                        'Formatted timestamps',
                        'WHO risk category classification',
                        'Dynamic table naming'
                    ],
                    'processing_time_seconds': round(processing_time, 2),
                    'timestamp': self._format_timestamp(datetime.now())
                })
                
            finally:
                client.close()
                
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(error_msg)
            return JsonResponse({
                'success': False,
                'error': error_msg,
                'timestamp': self._format_timestamp(datetime.now())
            }, status=500)
    
    def post(self, request):
        """Handle POST requests with enhanced parameters"""
        try:
            data = json.loads(request.body)
        except json.JSONDecodeError:
            return JsonResponse({
                'success': False,
                'error': 'Invalid JSON format in request body',
                'timestamp': self._format_timestamp(datetime.now())
            }, status=400)
        
        # Convert POST data to GET format
        years = data.get('years', 'all')
        if isinstance(years, list):
            years = ','.join(map(str, years))
        
        # Create mock GET request with enhanced parameters
        request.GET = type('MockGET', (), {
            'get': lambda self, key, default='': {
                'province': data.get('province', ''),
                'district': data.get('district', ''),
                'years': str(years),
                'show_available': str(data.get('show_available', False)).lower(),
                'save_to_postgres': str(data.get('save_to_postgres', True)).lower(),
                'update_mode': data.get('update_mode', 'replace').lower()  # Add update mode support
            }.get(key, default)
        })()
        
        return self.get(request)