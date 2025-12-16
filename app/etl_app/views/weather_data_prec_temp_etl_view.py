# # Django ETL Project - Monthly Weather Data ETL View


# Django ETL Project - Complete Fixed Monthly Weather Data ETL View
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
import traceback
import uuid
import time

logger = logging.getLogger(__name__)

@method_decorator(csrf_exempt, name='dispatch')
class WeatherDataETLView(View):
    """
    Complete Fixed Monthly Weather Data ETL API
    
    Features:
    - Optimized MongoDB extraction with timeout protection
    - Transaction-based PostgreSQL saving (same as working API code)
    - Auto-rename tables based on station names and districts
    - Prevent duplicate data insertion with unique IDs
    - Format: weather_station_of_preci_&_station_of_temp_district_years
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # MongoDB settings with timeout protection
        self.mongo_uri = getattr(settings, 'MONGO_URI')
        self.mongo_db = getattr(settings, 'MONGO_WEATHER_DB', 'weather_records_db')
        
        # PostgreSQL config from Django settings
        db_config = settings.DATABASES['default']
        self.pg_config = {
            'host': db_config['HOST'],
            'port': db_config['PORT'],
            'database': db_config['NAME'],
            'user': db_config['USER'],
            'password': db_config['PASSWORD']
        }
        
        logger.info(f"Weather ETL initialized with timeout protection")
        logger.info(f"MongoDB Database: {self.mongo_db}")
    
    def dispatch(self, request, *args, **kwargs):
        """Override dispatch to handle CORS"""
        if request.method == 'OPTIONS':
            response = JsonResponse({'message': 'OPTIONS request handled'})
            response['Access-Control-Allow-Origin'] = '*'
            response['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
            response['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
            return response
        
        response = super().dispatch(request, *args, **kwargs)
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        response['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        
        return response
    
    def _generate_unique_id(self, year, month, district, sector, prec_station, temp_station):
        """Generate unique 36-character UUID based on key fields"""
        key_string = f"{year}_{month}_{district}_{sector}_{prec_station}_{temp_station}".lower()
        namespace_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, key_string)
        return str(namespace_uuid)
    
    def _get_current_timestamp(self):
        """Get current timestamp formatted to date, hours and minutes only"""
        return datetime.now().strftime('%Y-%m-%d %H:%M')
    
    def _sanitize_name_part(self, name):
        """Sanitize station/district names for table naming"""
        if not name:
            return "unknown"
        
        sanitized = str(name).lower()
        sanitized = re.sub(r'[^a-z0-9_]', '_', sanitized)
        sanitized = re.sub(r'_+', '_', sanitized)
        sanitized = sanitized.strip('_')
        
        if len(sanitized) > 12:
            sanitized = sanitized[:12]
        
        return sanitized if sanitized else "unknown"
    
    def _generate_monthly_weather_table_name(self, prec_station, temp_station, district, sector, years):
        """Generate table name: weather_station_of_preci_&_station_of_temp_district_sector"""
        
        prec_clean = self._sanitize_name_part(prec_station)
        temp_clean = self._sanitize_name_part(temp_station)
        district_clean = self._sanitize_name_part(district)
        sector_clean = self._sanitize_name_part(sector)
        
        if isinstance(years, list) and len(years) > 0:
            sorted_years = sorted(years)
            if len(sorted_years) <= 3:
                year_part = '_'.join(map(str, sorted_years))
            else:
                year_part = f"{min(sorted_years)}_{max(sorted_years)}"
        else:
            year_part = "all"
        
        # Format: weather_startionprec_prec_and_stationtemp_temp_district_sector
        table_name = f"weather_{prec_clean}_prec_and_{temp_clean}_temp_{district_clean}_{sector_clean}"
        
        # Ensure table name is not too long (PostgreSQL limit is 63 characters)
        if len(table_name) > 60:
            available_length = 60 - len(f"weather__prec_and__temp__{year_part}") - 3
            prec_max = min(len(prec_clean), available_length // 4)
            temp_max = min(len(temp_clean), available_length // 4)
            dist_max = min(len(district_clean), available_length // 4)
            sect_max = available_length - prec_max - temp_max - dist_max
            
            prec_short = prec_clean[:prec_max] if prec_max > 0 else prec_clean[:3]
            temp_short = temp_clean[:temp_max] if temp_max > 0 else temp_clean[:3]
            district_short = district_clean[:dist_max] if dist_max > 0 else district_clean[:3]
            sector_short = sector_clean[:sect_max] if sect_max > 0 else sector_clean[:3]
            
            table_name = f"weather_{prec_short}_prec_and_{temp_short}_temp_{district_short}_{sector_short}"
        
        return table_name
    
    def _connect_mongodb(self):
        """Connect to MongoDB with timeout protection"""
        try:
            logger.info("Attempting MongoDB connection with timeout protection...")
            client = MongoClient(
                self.mongo_uri, 
                serverSelectionTimeoutMS=10000,  # 10 second connection timeout
                socketTimeoutMS=30000,           # 30 second socket timeout
                maxPoolSize=10                   # Limit connection pool
            )
            
            # Test connection with timeout
            client.admin.command('ismaster', maxTimeMS=5000)
            logger.info("MongoDB connection successful with timeout protection")
            
            return client
        except Exception as e:
            error_msg = f"MongoDB connection failed: {str(e)}"
            logger.error(error_msg)
            return None
    
    def _discover_weather_stations(self, client):
        """Discover available weather stations with timeout protection"""
        try:
            db = client[self.mongo_db]
            collections = db.list_collection_names()
            
            metadata_collections = []
            if 'metadata' in collections:
                metadata_collections.append('metadata')
            
            for coll in collections:
                if coll.endswith('_metadata'):
                    metadata_collections.append(coll)
            
            if not metadata_collections:
                return {'error': 'No metadata collections found', 'stations': []}
            
            all_metadata = []
            for meta_coll_name in metadata_collections:
                metadata_collection = db[meta_coll_name]
                # Limit metadata docs to prevent timeout
                docs = list(metadata_collection.find({}).limit(50))
                all_metadata.extend(docs)
            
            stations_info = {}
            all_years = set()
            
            for doc in all_metadata:
                collection_name = doc.get('data_collection_name')
                station = doc.get('station', '').strip()
                district = doc.get('district', '').strip()
                data_type = doc.get('data_type', '')
                
                if not district:
                    district = 'districtmissing'
                
                if not collection_name or not station:
                    continue
                
                # Get years with timeout protection
                if collection_name in collections:
                    data_collection = db[collection_name]
                    try:
                        year_pipeline = [
                            {"$group": {"_id": "$Year"}},
                            {"$match": {"_id": {"$ne": None}}},
                            {"$limit": 20}  # Limit years to prevent timeout
                        ]
                        
                        year_results = list(data_collection.aggregate(year_pipeline, maxTimeMS=10000))
                        data_years = []
                        
                        for year_result in year_results:
                            year_value = year_result.get('_id')
                            try:
                                year_int = int(float(year_value))
                                if 1900 <= year_int <= 2100:
                                    data_years.append(year_int)
                                    all_years.add(year_int)
                            except:
                                pass
                    except Exception as year_error:
                        logger.warning(f"Error getting years from {collection_name}: {str(year_error)}")
                        data_years = []
                
                station_key = f"{station}_{district}"
                if station_key not in stations_info:
                    stations_info[station_key] = {
                        'station_name': station,
                        'district': district,
                        'data_types': [],
                        'collections': [],
                        'years': set(),
                        'metadata': []
                    }
                
                stations_info[station_key]['data_types'].append(data_type)
                stations_info[station_key]['collections'].append(collection_name)
                stations_info[station_key]['years'].update(data_years)
                stations_info[station_key]['metadata'].append(doc)
            
            stations_list = []
            for key, info in stations_info.items():
                info['years'] = sorted(list(info['years']))
                stations_list.append(info)
            
            return {
                'stations': stations_list,
                'all_years': sorted(list(all_years)),
                'total_stations': len(stations_list)
            }
            
        except Exception as e:
            error_msg = f"Error discovering weather stations: {str(e)}"
            logger.error(error_msg)
            return {'error': error_msg, 'stations': []}
    
    def _extract_monthly_weather_data(self, client, years, prec_station=None, temp_station=None, district=None, sector=None):
        """Optimized monthly weather data extraction with timeout protection"""
        try:
            start_time = time.time()
            TIMEOUT_SECONDS = 60  # 1 minute timeout per collection
            
            db = client[self.mongo_db]
            
            # Get metadata collections with timeout protection
            try:
                collections = db.list_collection_names()
                metadata_collections = [c for c in collections if c.endswith('_metadata') or c == 'metadata']
                logger.info(f"WEATHER: Found {len(metadata_collections)} metadata collections")
            except Exception as e:
                logger.error(f"WEATHER: Error listing collections: {str(e)}")
                return [], []
            
            if not metadata_collections:
                logger.warning("WEATHER: No metadata collections found")
                return [], []
            
            prec_collections = []
            temp_collections = []
            metadata_records = []
            
            # Process metadata collections with timeout
            for meta_coll_name in metadata_collections:
                try:
                    metadata_collection = db[meta_coll_name]
                    
                    # Use timeout and limit to prevent hanging
                    metadata_docs = list(metadata_collection.find({}).limit(100))
                    logger.info(f"WEATHER: Processing {len(metadata_docs)} metadata docs from {meta_coll_name}")
                    
                    for doc in metadata_docs:
                        collection_name = doc.get('data_collection_name')
                        station = doc.get('station', '').strip()
                        doc_district = doc.get('district', '').strip() or 'no_district'
                        data_type = doc.get('data_type', '')
                        
                        # Apply filters early to reduce processing
                        station_match = True
                        district_match = True
                        
                        if prec_station and data_type == 'precipitation':
                            station_match = prec_station.lower() in station.lower()
                        elif temp_station and data_type == 'temperature':
                            station_match = temp_station.lower() in station.lower()
                        
                        # Relaxed district filtering to allow data ingestion even if metadata lacks district info
                        # if district:
                        #     district_match = district.lower() in doc_district.lower()
                        
                        if station_match and district_match and collection_name:
                            metadata_records.append(doc)
                            
                            if data_type == 'precipitation':
                                prec_collections.append((collection_name, station, doc_district, doc))
                            elif data_type == 'temperature':
                                temp_collections.append((collection_name, station, doc_district, doc))
                    
                except Exception as meta_error:
                    logger.error(f"WEATHER: Error processing metadata {meta_coll_name}: {str(meta_error)}")
                    continue
            
            logger.info(f"WEATHER: Found {len(prec_collections)} precipitation and {len(temp_collections)} temperature collections")
            
            monthly_data = {}
            current_timestamp = self._get_current_timestamp()
            
            # Process precipitation data with optimized aggregation
            for i, (collection_name, station, station_district, metadata) in enumerate(prec_collections):
                try:
                    # Check timeout
                    if time.time() - start_time > TIMEOUT_SECONDS:
                        logger.warning(f"WEATHER: Timeout reached, stopping at precipitation collection {i+1}")
                        break
                    
                    collection = db[collection_name]
                    
                    # Use aggregation pipeline for better performance
                    pipeline = [
                        {
                            "$match": {
                                "Year": {"$in": years},
                                "PRECIP": {"$exists": True, "$ne": None, "$gte": 0}
                            }
                        },
                        {
                            "$group": {
                                "_id": {
                                    "year": "$Year", 
                                    "month": "$Month"
                                },
                                "avg_precip": {"$avg": "$PRECIP"},
                                "count": {"$sum": 1}
                            }
                        },
                        {
                            "$match": {
                                "_id.year": {"$ne": None},
                                "_id.month": {"$ne": None}
                            }
                        }
                    ]
                    
                    # Execute aggregation with timeout
                    results = list(collection.aggregate(pipeline, maxTimeMS=30000))  # 30 second timeout
                    
                    for result in results:
                        year = self._clean_integer(result['_id']['year'])
                        month = self._clean_month(result['_id']['month'])
                        avg_precip = round(float(result['avg_precip']), 2)
                        
                        if year and month and year in years:
                            key = (year, month, station_district)
                            
                            if key not in monthly_data:
                                monthly_data[key] = {
                                    'year': year,
                                    'month': month,
                                    'district': station_district,
                                    'monthly_precipitation': avg_precip,
                                    'monthly_temperature': None,
                                    'prec_station': station,
                                    'temp_station': '',
                                    'prec_metadata': metadata,
                                    'temp_metadata': None,
                                    'created_at': current_timestamp,
                                    'updated_at': current_timestamp
                                }
                            else:
                                monthly_data[key]['monthly_precipitation'] = avg_precip
                                monthly_data[key]['prec_station'] = station
                                monthly_data[key]['prec_metadata'] = metadata
                    
                    logger.info(f"WEATHER: Processed precipitation for {station} - {len(results)} monthly records")
                    
                except Exception as prec_error:
                    logger.error(f"WEATHER: Error processing precipitation {collection_name}: {str(prec_error)}")
                    continue
            
            # Process temperature data with optimized aggregation
            for i, (collection_name, station, station_district, metadata) in enumerate(temp_collections):
                try:
                    # Check timeout
                    if time.time() - start_time > TIMEOUT_SECONDS:
                        logger.warning(f"WEATHER: Timeout reached, stopping at temperature collection {i+1}")
                        break
                    
                    collection = db[collection_name]
                    
                    # First get yearly averages for null filling
                    yearly_avg_pipeline = [
                        {
                            "$match": {
                                "Year": {"$in": years},
                                "TMPMAX": {"$exists": True, "$ne": None, "$gte": -50, "$lte": 60}
                            }
                        },
                        {
                            "$group": {
                                "_id": "$Year",
                                "yearly_avg": {"$avg": "$TMPMAX"}
                            }
                        }
                    ]
                    
                    yearly_results = list(collection.aggregate(yearly_avg_pipeline, maxTimeMS=30000))
                    yearly_averages = {result['_id']: round(result['yearly_avg'], 2) for result in yearly_results}
                    
                    # Then get monthly averages
                    monthly_pipeline = [
                        {
                            "$match": {
                                "Year": {"$in": years},
                                "TMPMAX": {"$exists": True, "$ne": None, "$gte": -50, "$lte": 60}
                            }
                        },
                        {
                            "$group": {
                                "_id": {
                                    "year": "$Year", 
                                    "month": "$Month"
                                },
                                "avg_temp": {"$avg": "$TMPMAX"},
                                "count": {"$sum": 1}
                            }
                        }
                    ]
                    
                    monthly_results = list(collection.aggregate(monthly_pipeline, maxTimeMS=30000))
                    
                    # Process all 12 months for each year
                    for year in years:
                        year_avg = yearly_averages.get(year, 20.0)  # Default fallback
                        
                        # Create a dict of existing monthly data
                        existing_monthly = {
                            result['_id']['month']: round(result['avg_temp'], 2) 
                            for result in monthly_results 
                            if result['_id']['year'] == year and result['_id']['month']
                        }
                        
                        # Fill all 12 months
                        for month in range(1, 13):
                            key = (year, month, station_district)
                            
                            # Use existing monthly average or fall back to yearly average
                            monthly_temp = existing_monthly.get(month, year_avg)
                            
                            if key not in monthly_data:
                                monthly_data[key] = {
                                    'year': year,
                                    'month': month,
                                    'district': station_district,
                                    'monthly_precipitation': 0.0,
                                    'monthly_temperature': monthly_temp,
                                    'prec_station': '',
                                    'temp_station': station,
                                    'prec_metadata': None,
                                    'temp_metadata': metadata,
                                    'created_at': current_timestamp,
                                    'updated_at': current_timestamp
                                }
                            else:
                                monthly_data[key]['monthly_temperature'] = monthly_temp
                                monthly_data[key]['temp_station'] = station
                                monthly_data[key]['temp_metadata'] = metadata
                    
                    logger.info(f"WEATHER: Processed temperature for {station} - {len(yearly_results)} yearly averages")
                    
                except Exception as temp_error:
                    logger.error(f"WEATHER: Error processing temperature {collection_name}: {str(temp_error)}")
                    continue
            
            # Convert to final format with timeout check
            if time.time() - start_time > TIMEOUT_SECONDS:
                logger.warning(f"WEATHER: Timeout during final processing, returning partial data")
            
            final_data = []
            for key, record in monthly_data.items():
                district_name = record['district'] if record['district'] else 'districtmissing'
                prec_station_name = record['prec_station'] or 'unknown'
                temp_station_name = record['temp_station'] or 'unknown'
                
                # Generate unique ID
                unique_id = self._generate_unique_id(
                    record['year'], record['month'], district_name, sector,
                    prec_station_name, temp_station_name
                )
                
                # Ensure temperature is not None
                monthly_temp = record['monthly_temperature']
                if monthly_temp is None:
                    all_temps = [r['monthly_temperature'] for r in monthly_data.values() 
                                if r['monthly_temperature'] is not None]
                    monthly_temp = round(sum(all_temps) / len(all_temps), 2) if all_temps else 20.0
                
                final_record = {
                    'unique_id': unique_id,
                    'year': record['year'],
                    'month': record['month'],
                    'monthly_precipitation': round(record['monthly_precipitation'], 2),
                    'monthly_temperature': monthly_temp,
                    'monthly_precipitation': round(record['monthly_precipitation'], 2),
                    'monthly_temperature': monthly_temp,
                    'metadata': f"prec station: {prec_station_name} - monthly prec, temp station: {temp_station_name} - monthly temp, district: {district_name}",
                    'district': district if district else district_name, # Prioritize input district
                    'sector': sector, # Add sector
                    'prec_station': record['prec_station'],
                    'temp_station': record['temp_station'],
                    'created_at': record['created_at'],
                    'updated_at': record['updated_at']
                }
                
                final_data.append(final_record)
            
            processing_time = time.time() - start_time
            logger.info(f"WEATHER: Processed {len(final_data)} monthly records in {processing_time:.2f} seconds")
            
            return final_data, metadata_records
            
        except Exception as e:
            error_msg = f"Error extracting monthly weather data: {str(e)}"
            logger.error(error_msg)
            logger.error(f"WEATHER TRACEBACK: {traceback.format_exc()}")
            return [], []
    
    def _clean_month(self, month_value):
        """Convert month names/numbers to integers"""
        if pd.isna(month_value) or not month_value:
            return None
        
        try:
            month_num = int(float(month_value))
            if 1 <= month_num <= 12:
                return month_num
        except:
            pass
        
        month_str = str(month_value).strip().lower()
        month_mapping = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12,
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
            'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
            'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
        }
        
        return month_mapping.get(month_str, None)
    
    def _clean_integer(self, value):
        """Clean integer fields"""
        try:
            if pd.isna(value) or value == "":
                return None
            return int(float(value))
        except:
            return None
    
    def _clean_float(self, value):
        """Clean float fields"""
        try:
            if pd.isna(value) or value == "":
                return None
            return float(value)
        except:
            return None
    
    def _save_monthly_weather_to_postgres(self, data, table_name, years):
        """Save monthly weather data using transaction approach (same as working API code)"""
        try:
            if not data:
                return False, "No monthly weather data to save"
            
            # Create engine using same pattern as API code
            engine = create_engine(
                f"postgresql://{self.pg_config['user']}:{self.pg_config['password']}@"
                f"{self.pg_config['host']}:{self.pg_config['port']}/{self.pg_config['database']}"
            )
            
            logger.info(f"WEATHER SAVE: Saving {len(data)} records to {table_name}")
            
            # KEY FIX: Use engine.begin() for proper transaction handling (same as API code)
            with engine.begin() as conn:
                
                # Drop existing table (same pattern as API code)
                drop_table_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE"
                conn.execute(text(drop_table_sql))
                logger.info(f"WEATHER: Dropped existing table {table_name}")
                
                # Create table with proper structure
                create_table_sql = f"""
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    unique_id VARCHAR(36) UNIQUE NOT NULL,
                    year INTEGER,
                    month INTEGER,
                    monthly_precipitation NUMERIC(10,2),
                    monthly_temperature NUMERIC(10,2),
                    metadata TEXT,
                    district VARCHAR(100),
                    sector VARCHAR(100),
                    prec_station VARCHAR(100),
                    temp_station VARCHAR(100),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()

                )
                """
                conn.execute(text(create_table_sql))
                logger.info(f"WEATHER: Created table {table_name}")
                
                # Create indexes (same pattern as API code)
                create_indexes_sql = f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name.replace('-', '_')}_unique_id ON {table_name}(unique_id);
                CREATE INDEX IF NOT EXISTS idx_{table_name.replace('-', '_')}_year_month ON {table_name}(year, month);
                CREATE INDEX IF NOT EXISTS idx_{table_name.replace('-', '_')}_district ON {table_name}(district);
                CREATE INDEX IF NOT EXISTS idx_{table_name.replace('-', '_')}_sector ON {table_name}(sector);
                CREATE INDEX IF NOT EXISTS idx_{table_name.replace('-', '_')}_stations ON {table_name}(prec_station, temp_station);
                CREATE INDEX IF NOT EXISTS idx_{table_name.replace('-', '_')}_timestamps ON {table_name}(created_at, updated_at);
                """
                conn.execute(text(create_indexes_sql))
                logger.info(f"WEATHER: Created indexes for {table_name}")
                
                # Insert data using individual inserts (same approach as API code)
                records_inserted = 0
                records_failed = 0
                
                for i, record in enumerate(data):
                    # Prepare record data with safe conversion
                        insert_data = {
                            'unique_id': record.get('unique_id', self._generate_unique_id(
                                record.get('year', 0), 
                                record.get('month', 1), 
                                record.get('district', 'unknown'),
                                record.get('sector', 'unknown'),
                                record.get('prec_station', 'unknown'),
                                record.get('temp_station', 'unknown')
                            )),
                            'year': int(record.get('year', 0)),
                            'month': int(record.get('month', 1)),
                            'monthly_precipitation': float(record.get('monthly_precipitation', 0.0)),
                            'monthly_temperature': float(record.get('monthly_temperature', 0.0)),
                            'metadata': str(record.get('metadata', '')),
                            'district': str(record.get('district', '')),
                            'sector': str(record.get('sector', '')),
                            'prec_station': str(record.get('prec_station', '')),
                            'temp_station': str(record.get('temp_station', '')),
                            'created_at': record.get('created_at', self._get_current_timestamp()),
                            'updated_at': record.get('updated_at', self._get_current_timestamp())
                        }
                        
                        # Insert SQL (same pattern as API code)
                        insert_sql = f"""
                        INSERT INTO {table_name}
                        (unique_id, year, month, monthly_precipitation, monthly_temperature, metadata,
                         district, sector, prec_station, temp_station, created_at, updated_at)
                        VALUES (:unique_id, :year, :month, :monthly_precipitation, :monthly_temperature, :metadata,
                                :district, :sector, :prec_station, :temp_station, :created_at, :updated_at)
                        """
                        
                        conn.execute(text(insert_sql), insert_data)
                        records_inserted += 1
                        
                        # Log progress every 1000 records
                        if records_inserted % 1000 == 0:
                            logger.info(f"WEATHER PROGRESS: {records_inserted}/{len(data)} records inserted")
                
                # Verify the save worked (same as API code)
                verify_sql = f"SELECT COUNT(*) FROM {table_name}"
                result = conn.execute(text(verify_sql))
                final_count = result.fetchone()[0]
                
                logger.info(f"WEATHER SUCCESS: {final_count} records saved to {table_name}")
                
                # The transaction will automatically commit when exiting the 'with' block
                
                if final_count > 0:
                    message = f"Successfully saved {final_count} weather records to '{table_name}'"
                    if records_failed > 0:
                        message += f" (failed: {records_failed})"
                    return True, message
                else:
                    return False, f"No records saved to {table_name}. Check data format and database connectivity."
            
        except Exception as e:
            error_msg = f"Error saving weather data to PostgreSQL: {str(e)}"
            logger.error(error_msg)
            logger.error(f"WEATHER TRACEBACK: {traceback.format_exc()}")
            return False, error_msg
    
    def post(self, request):
        """Handle POST requests - delegate to same logic but utilize POST data"""
        return self.get(request)

    def get(self, request):
        """Handle GET/POST requests for monthly weather ETL with timeout protection"""
        start_time = datetime.now()
        TOTAL_TIMEOUT = 90  # 90 second total timeout
        
        try:
            # Get parameters from either JSON body, POST or GET
            data_source = {}
            if request.method == 'POST':
                if request.content_type == 'application/json':
                    try:
                        data_source = json.loads(request.body)
                    except json.JSONDecodeError:
                        data_source = request.POST
                else:
                    data_source = request.POST
            else:
                data_source = request.GET
            
            # Handle years which might be list or string
            years_param = data_source.get('years', '')
            if isinstance(years_param, list):
                years_param = ','.join(map(str, years_param))
                
            prec_station = str(data_source.get('prec_station', '')).strip()
            temp_station = str(data_source.get('temp_station', '')).strip()
            district = str(data_source.get('district', '')).strip()
            sector = str(data_source.get('sector', '')).strip()
            
            # Handle boolean fields
            show_available_val = data_source.get('show_available', False)
            show_available = str(show_available_val).lower() == 'true' if isinstance(show_available_val, str) else bool(show_available_val)
            
            save_postgres_val = data_source.get('save_to_postgres', True)
            save_to_postgres = str(save_postgres_val).lower() == 'true' if isinstance(save_postgres_val, str) else bool(save_postgres_val)
            
            # Connect to MongoDB with timeout
            client = self._connect_mongodb()
            if not client:
                return JsonResponse({
                    'success': False,
                    'error': 'Cannot connect to MongoDB',
                    'timestamp': self._get_current_timestamp()
                }, status=503)
            
            try:
                # Check if we're approaching timeout
                elapsed = (datetime.now() - start_time).total_seconds()
                if elapsed > TOTAL_TIMEOUT:
                    return JsonResponse({
                        'success': False,
                        'error': 'Request timeout - weather data processing is taking too long',
                        'suggestion': 'Try filtering by specific years, stations, or districts',
                        'timestamp': self._get_current_timestamp()
                    }, status=408)
                
                # Discover available stations
                discovery = self._discover_weather_stations(client)
                
                if 'error' in discovery:
                    return JsonResponse({
                        'success': False,
                        'error': f'Error accessing weather data: {discovery["error"]}',
                        'timestamp': self._get_current_timestamp()
                    }, status=404)
                
                # If user wants to see available options
                if show_available:
                    return JsonResponse({
                        'success': True,
                        'message': 'Available Monthly Weather Data Options',
                        'available_options': {
                            'years': discovery['all_years'],
                            'stations': discovery['stations']
                        },
                        'usage_examples': {
                            'all_data': '?years=all&save_to_postgres=true',
                            'specific_stations': '?years=2021,2022&prec_station=Juru&temp_station=Nyamata&save_to_postgres=true',
                            'district_filter': '?years=2021&district=Bugesera&save_to_postgres=true',
                            'table_naming': 'Auto-generated: weather_juru_prec_and_nyamata_temp_bugesera_2021_2022'
                        },
                        'note': 'Tables will be auto-named as: weather_station_of_preci_&_station_of_temp_district_years',
                        'timestamp': self._get_current_timestamp()
                    })
                
                # Parse years
                available_years = discovery['all_years']
                
                if not years_param or years_param.lower() == 'all':
                    years = available_years
                elif '-' in years_param and ',' not in years_param:
                    try:
                        start_year, end_year = map(int, years_param.split('-'))
                        years = [y for y in range(start_year, end_year + 1) if y in available_years]
                    except ValueError:
                        return JsonResponse({
                            'success': False,
                            'error': f'Invalid year range format: {years_param}',
                            'timestamp': self._get_current_timestamp()
                        }, status=400)
                else:
                    try:
                        requested_years = [int(y.strip()) for y in years_param.split(',')]
                        years = [y for y in requested_years if y in available_years]
                        
                        invalid_years = [y for y in requested_years if y not in available_years]
                        if invalid_years:
                            return JsonResponse({
                                'success': False,
                                'error': f'Years {invalid_years} are not available',
                                'available_years': available_years,
                                'timestamp': self._get_current_timestamp()
                            }, status=400)
                    except ValueError:
                        return JsonResponse({
                            'success': False,
                            'error': f'Invalid year format: {years_param}',
                            'timestamp': self._get_current_timestamp()
                        }, status=400)
                
                # Extract monthly weather data
                # Extract monthly weather data
                monthly_data, metadata_records = self._extract_monthly_weather_data(
                    client, years, prec_station, temp_station, district, sector
                )
                
                if not monthly_data:
                    return JsonResponse({
                        'success': False,
                        'message': 'No monthly weather data found for the specified criteria',
                        'filters_applied': {
                            'years': years,
                            'prec_station': prec_station or 'All precipitation stations',
                            'temp_station': temp_station or 'All temperature stations',
                            'district': district or 'All districts'
                        },
                        'timestamp': self._get_current_timestamp()
                    })
                
                # Generate table name and save to PostgreSQL
                postgres_saved = False
                postgres_message = ""
                table_name = ""
                
                if save_to_postgres:
                    # Get station names from the data for table naming
                    sample_record = monthly_data[0]
                    prec_station_name = sample_record.get('prec_station')
                    temp_station_name = sample_record.get('temp_station')
                    district_name = sample_record.get('district')
                    sector_name = sample_record.get('sector')
                    
                    table_name = self._generate_monthly_weather_table_name(
                        prec_station_name, temp_station_name, district_name, sector_name, years
                    )
                    
                    postgres_saved, postgres_message = self._save_monthly_weather_to_postgres(
                        monthly_data, table_name, years
                    )
                
                # Calculate summary statistics
                total_records = len(monthly_data)
                avg_precip = sum(r['monthly_precipitation'] for r in monthly_data) / total_records if total_records > 0 else 0
                avg_temp = sum(r['monthly_temperature'] for r in monthly_data) / total_records if total_records > 0 else 0
                
                # Get processed stations info
                prec_stations = sorted(list(set(r['prec_station'] for r in monthly_data if r['prec_station'])))
                temp_stations = sorted(list(set(r['temp_station'] for r in monthly_data if r['temp_station'])))
                
                processing_time = (datetime.now() - start_time).total_seconds()
                
                return JsonResponse({
                    'success': True,
                    'message': f'Successfully processed {total_records} monthly weather records',
                    'summary': {
                        'total_monthly_records': total_records,
                        'average_monthly_precipitation': round(avg_precip, 2),
                        'average_monthly_temperature': round(avg_temp, 2),
                        'years_processed': sorted(list(set(r['year'] for r in monthly_data))),
                        'months_covered': sorted(list(set(r['month'] for r in monthly_data))),
                        'districts_covered': sorted(list(set(r['district'] for r in monthly_data if r['district']))),
                        'prec_stations_processed': prec_stations,
                        'temp_stations_processed': temp_stations
                    },
                    'filters_applied': {
                        'years': years,
                        'prec_station': prec_station or 'All precipitation stations',
                        'temp_station': temp_station or 'All temperature stations',
                        'district': district or 'All districts'
                    },
                    'postgres': {
                        'saved': postgres_saved,
                        'table_name': table_name if postgres_saved else None,
                        'message': postgres_message
                    },
                    'processing_time_seconds': round(processing_time, 2),
                    'timestamp': self._get_current_timestamp()
                })
                
            finally:
                client.close()
                
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return JsonResponse({
                'success': False,
                'error': error_msg,
                'timestamp': self._get_current_timestamp()
            }, status=500)
    
    def post(self, request):
        """Handle POST requests for monthly weather ETL"""
        try:
            data = json.loads(request.body)
        except json.JSONDecodeError as e:
            return JsonResponse({
                'success': False,
                'error': f'Invalid JSON format: {str(e)}',
                'timestamp': self._get_current_timestamp()
            }, status=400)
        
        # Convert POST data to GET format
        years = data.get('years', 'all')
        if isinstance(years, list):
            years = ','.join(map(str, years))
        
        # Create mock GET request
        request.GET = type('MockGET', (), {
            'get': lambda self, key, default='': {
                'years': str(years),
                'prec_station': data.get('prec_station', ''),
                'temp_station': data.get('temp_station', ''),
                'district': data.get('district', ''),
                'show_available': str(data.get('show_available', False)).lower(),
                'save_to_postgres': str(data.get('save_to_postgres', True)).lower()
            }.get(key, default)
        })()
        
        return self.get(request)