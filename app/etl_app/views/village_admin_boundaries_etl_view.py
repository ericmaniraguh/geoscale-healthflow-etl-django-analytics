# etl_app/views/village_admin_boundaries_etl_view.py
import json
import logging
import traceback
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from django.conf import settings
from django.contrib import messages
from django.http import JsonResponse
from django.shortcuts import redirect
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from pymongo import MongoClient
from sqlalchemy import create_engine, text
import re

logger = logging.getLogger(__name__)

@method_decorator(csrf_exempt, name='dispatch')
class VillageAdminBoundariesETLView(View):
    """ETL View for Rwanda Administrative Boundaries from MongoDB to PostgreSQL"""
    
    def __init__(self):
        super().__init__()
        # MongoDB connection settings
        self.mongo_uri = getattr(settings, 'MONGO_SHAPEFILE_URI')
        self.mongo_db = getattr(settings, 'MONGO_SHAPEFILE_DB', 'geospatial_wgs84_boundaries_db')
        self.mongo_collection = getattr(settings, 'MONGO_SHAPEFILE_COLLECTION', 'boundaries_slope_wgs84')
        
        # PostgreSQL staging database settings
        self.pg_config = {
            'host': getattr(settings, 'STAGING_DB_HOST', 'localhost'),
            'port': getattr(settings, 'STAGING_DB_PORT', 5433),
            'database': getattr(settings, 'STAGING_DB_NAME', 'data_pipeline_hc_staging_db'),
            'user': getattr(settings, 'STAGING_DB_USER', 'postgres'),
            'password': getattr(settings, 'STAGING_DB_PASSWORD', 'admin'),
        }

    def connect_mongodb(self) -> Optional[MongoClient]:
        """Connect to MongoDB using the shapefile URI"""
        try:
            masked_uri = self.mongo_uri.split('@')[-1] if '@' in self.mongo_uri else self.mongo_uri
            logger.info(f"Connecting to MongoDB: URI=...@{masked_uri}, DB={self.mongo_db}, Collection={self.mongo_collection}")
            
            client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=30000)
            client.admin.command('ismaster')
            logger.info(f"Successfully connected to MongoDB: {self.mongo_db}")
            
            # Debug: Check collection directly immediately after connection
            count = client[self.mongo_db][self.mongo_collection].count_documents({})
            logger.info(f"Direct connection check - Documents in {self.mongo_collection}: {count}")
            
            return client
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            return None

    def analyze_collection_structure(self, client: MongoClient) -> Dict:
        """Analyze the structure of the boundaries collection"""
        try:
            db = client[self.mongo_db]
            collection = db[self.mongo_collection]
            
            # Get total count
            total_count = collection.count_documents({})
            
            # Get sample document
            sample_doc = collection.find_one({})
            
            if not sample_doc:
                return {
                    'success': False,
                    'error': f'No documents found in collection {self.mongo_collection}'
                }
            
            # Analyze structure based on your sample data
            analysis = {
                'success': True,
                'total_documents': total_count,
                'has_geometry': 'geometry' in sample_doc,
                'geometry_type': sample_doc.get('geometry', {}).get('type'),
                'has_slope_data': 'mean_slope' in sample_doc,
                'has_admin_data': 'District' in sample_doc,
                'sample_provinces': [],
                'sample_districts': [],
                'sample_sectors': [],
                'available_fields': list(sample_doc.keys())
            }
            
            # Get unique administrative values
            try:
                provinces = list(collection.distinct("Province"))
                analysis['sample_provinces'] = [p for p in provinces if p][:10]
                
                districts = list(collection.distinct("District"))
                analysis['sample_districts'] = [d for d in districts if d][:20]
                
                sectors = list(collection.distinct("Sector_1"))
                analysis['sample_sectors'] = [s for s in sectors if s][:30]
            except Exception as e:
                logger.warning(f"Failed to get distinct values: {e}")
            
            return analysis
            
        except Exception as e:
            logger.error(f"Collection analysis failed: {e}")
            return {'success': False, 'error': str(e)}

    def extract_filtered_data(self, client: MongoClient, district: str = None, 
                            sector: str = None, province: str = None) -> Tuple[List[Dict], Dict]:
        """Extract data from MongoDB with filtering based on your data structure"""
        try:
            db = client[self.mongo_db]
            collection = db[self.mongo_collection]
            
            # Build query based on the actual field names in your data
            query = {}
            
            if province:
                # Try both Province and Prov_name fields
                query['$or'] = [
                    {"Province": {"$regex": f"^{re.escape(province)}$", "$options": "i"}},
                    {"Prov_name": {"$regex": f"^{re.escape(province)}$", "$options": "i"}},
                    {"Prov_Enlgi": {"$regex": f"^{re.escape(province)}$", "$options": "i"}}
                ]
            
            if district:
                query["District"] = {"$regex": f"^{re.escape(district)}$", "$options": "i"}
            
            if sector:
                query["Sector_1"] = {"$regex": f"^{re.escape(sector)}$", "$options": "i"}
            
            logger.info(f"Using MongoDB Query: {query}")
            
            # Execute query
            documents = list(collection.find(query))
            
            stats = {
                'total_documents': len(documents),
                'query_used': str(query),
                'filters_applied': {
                    'province': province,
                    'district': district,
                    'sector': sector
                }
            }
            
            # If no exact matches, try fuzzy matching
            if not documents and (district or sector):
                fuzzy_query = {}
                if district:
                    fuzzy_query["District"] = {"$regex": district, "$options": "i"}
                if sector:
                    fuzzy_query["Sector_1"] = {"$regex": sector, "$options": "i"}
                
                fuzzy_results = list(collection.find(fuzzy_query).limit(10))
                stats['fuzzy_matches'] = len(fuzzy_results)
                stats['suggestions'] = []
                
                for doc in fuzzy_results:
                    suggestion = {
                        'district': doc.get('District'),
                        'sector': doc.get('Sector_1'),
                        'province': doc.get('Province'),
                        'village': doc.get('Village')
                    }
                    stats['suggestions'].append(suggestion)
            
            return documents, stats
            
        except Exception as e:
            logger.error(f"Data extraction failed: {e}")
            return [], {'error': str(e)}

    def process_documents(self, documents: List[Dict]) -> Tuple[List[Dict], Dict]:
        """Process documents based on your exact data structure"""
        processed_records = []
        stats = {
            'total_documents': len(documents),
            'processed_successfully': 0,
            'processing_errors': 0,
            'slope_data_found': 0,
            'geometry_types': {}
        }
        
        for doc in documents:
            try:
                # Extract geometry
                geom = doc.get('geometry', {})
                geom_type = geom.get('type', 'Unknown')
                
                # Track geometry types
                stats['geometry_types'][geom_type] = stats['geometry_types'].get(geom_type, 0) + 1
                
                # Check if slope data exists
                if 'mean_slope' in doc:
                    stats['slope_data_found'] += 1
                
                # Calculate centroid for polygons
                centroid_lat, centroid_lon = self._calculate_centroid(geom)
                
                # Build record for PostgreSQL using your data structure
                record = {
                    'unique_id': str(uuid.uuid4()),
                    
                    # Original administrative codes and IDs
                    'feature_id': doc.get('feature_id'),
                    'objectid_1': doc.get('OBJECTID_1'),
                    'code_vill': doc.get('Code_vill_'),
                    'code_vill1': doc.get('Code_vill1'),
                    'zones_code': doc.get('Zones_Code'),
                    'ea_code': doc.get('EA_Code'),
                    
                    # Administrative hierarchy
                    'province_code': doc.get('Code_Prov'),
                    'province_name': doc.get('Province'),
                    'province_english': doc.get('Prov_Enlgi'),
                    'province_full': doc.get('Prov_name'),
                    'province_id': doc.get('Prov_ID'),
                    
                    'district_code': doc.get('code_Dist'),
                    'district_name': doc.get('District'),
                    'district_id': doc.get('District_I'),
                    
                    'sector_code': doc.get('Code_Sect'),
                    'sector_name': doc.get('Sector_1'),
                    'sector_id1': doc.get('Sect_ID1'),
                    'sector_id2': doc.get('Sect_ID2'),
                    
                    'cell_code': doc.get('Code_cell_'),
                    'cell_name': doc.get('Cellule_1'),
                    'cell_id1': doc.get('Cell_ID1'),
                    'cell_id2': doc.get('Cell_ID2'),
                    
                    'village_name': doc.get('Village'),
                    'village_id': doc.get('Village_ID'),
                    'village_id2': doc.get('Village__1'),
                    
                    # Population and household data
                    'population': self._safe_int_convert(doc.get('Population')),
                    'households': self._safe_int_convert(doc.get('Household')),
                    'sum_population': self._safe_int_convert(doc.get('SUM_Popula')),
                    'sum_households': self._safe_int_convert(doc.get('SUM_Househ')),
                    
                    # Area and shape measurements
                    'area_km': self._safe_float_convert(doc.get('Area_KM')),
                    'shape_length': self._safe_float_convert(doc.get('Shape_Leng')),
                    'shape_length_1': self._safe_float_convert(doc.get('Shape_Le_1')),
                    'shape_length_2': self._safe_float_convert(doc.get('Shape_Le_2')),
                    'shape_area': self._safe_float_convert(doc.get('Shape_Area')),
                    'shape_area_1': self._safe_float_convert(doc.get('Shape_Ar_1')),
                    
                    # Slope analysis data
                    'mean_slope': self._safe_float_convert(doc.get('mean_slope')),
                    'max_slope': self._safe_float_convert(doc.get('max_slope')),
                    'min_slope': self._safe_float_convert(doc.get('min_slope')),
                    'slope_class': doc.get('slope_class'),
                    'slope_points_used': self._safe_int_convert(doc.get('slope_points_used')),
                    
                    # Urban/Rural classification
                    'ur_name': doc.get('UR_Name'),
                    'district_council': self._safe_int_convert(doc.get('D_Council')),
                    'status': self._safe_int_convert(doc.get('Status')),
                    
                    # Coordinate system info
                    'coordinates_system': doc.get('coordinates_system'),
                    
                    # Geometry data
                    'geometry_type': geom_type,
                    'geometry_geojson': json.dumps(geom) if geom else None,
                    'centroid_lat': centroid_lat,
                    'centroid_lon': centroid_lon,
                    
                    # Processing metadata
                    'processing_metadata': json.dumps(doc.get('processing_metadata', {})),
                    'batch_info': json.dumps(doc.get('_batch_info', {})),
                    
                    # Metadata
                    'source_collection': self.mongo_collection,
                    'source_database': self.mongo_db,
                    'extracted_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                processed_records.append(record)
                stats['processed_successfully'] += 1
                
            except Exception as e:
                logger.error(f"Document processing error: {e}")
                stats['processing_errors'] += 1
                continue
        
        return processed_records, stats

    def _calculate_centroid(self, geometry: Dict) -> Tuple[Optional[float], Optional[float]]:
        """Calculate centroid from geometry"""
        try:
            if geometry.get('type') == 'Point':
                coords = geometry.get('coordinates', [])
                if len(coords) >= 2:
                    return coords[1], coords[0]  # lat, lon
                    
            elif geometry.get('type') == 'Polygon':
                coordinates = geometry.get('coordinates', [])
                if coordinates and len(coordinates) > 0:
                    # Get exterior ring
                    exterior_ring = coordinates[0]
                    if len(exterior_ring) > 0:
                        # Calculate centroid
                        lons = [coord[0] for coord in exterior_ring if len(coord) >= 2]
                        lats = [coord[1] for coord in exterior_ring if len(coord) >= 2]
                        if lons and lats:
                            return sum(lats) / len(lats), sum(lons) / len(lons)
        except Exception as e:
            logger.warning(f"Centroid calculation failed: {e}")
        
        return None, None

    def _safe_int_convert(self, value) -> Optional[int]:
        """Safely convert value to integer"""
        if value in (None, "", "None", "null"):
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _safe_float_convert(self, value) -> Optional[float]:
        """Safely convert value to float"""
        if value in (None, "", "None", "null"):
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def save_to_postgres(self, records: List[Dict], district: str = None, 
                        sector: str = None, update_mode: str = 'replace') -> Dict:
        """Save records to PostgreSQL - FIXED VERSION"""
        try:
            # Generate table name
            table_name = self._generate_table_name(district, sector)
            
            # Create SQLAlchemy engine
            connection_string = (
                f"postgresql://{self.pg_config['user']}:{self.pg_config['password']}"
                f"@{self.pg_config['host']}:{self.pg_config['port']}/{self.pg_config['database']}"
            )
            
            engine = create_engine(connection_string)
            
            # Use begin() for automatic transaction management (auto-commit on exit)
            with engine.begin() as conn:
                # Create or ensure table exists
                if update_mode == 'replace':
                    logger.info(f"Creating/replacing table: {table_name}")
                    self._create_table(conn, table_name)
                else:
                    logger.info(f"Ensuring table exists for append: {table_name}")
                    self._ensure_table_exists(conn, table_name)
                
                # Insert records
                logger.info(f"Inserting {len(records)} records into {table_name}")
                inserted_count = self._insert_records(conn, table_name, records, update_mode)
                
                logger.info(f"Successfully inserted {inserted_count} records")
                
                # Transaction auto-commits when exiting the 'with' block
                
                return {
                    'success': True,
                    'table_name': table_name,
                    'message': f'Successfully saved {inserted_count} records to table {table_name}',
                    'records_count': inserted_count,
                    'update_mode': update_mode
                }
                
        except Exception as e:
            logger.error(f"PostgreSQL save failed: {e}\n{traceback.format_exc()}")
            return {
                'success': False,
                'table_name': table_name if 'table_name' in locals() else 'unknown',
                'message': f'Failed to save to PostgreSQL: {str(e)}',
                'records_count': 0,
                'update_mode': update_mode,
                'error': str(e)
            }

    def _generate_table_name(self, district: str = None, sector: str = None) -> str:
        """Generate table name based on filters"""
        def sanitize(name: str) -> str:
            if not name:
                return "all"
            clean = re.sub(r'[^a-zA-Z0-9_]', '_', name.lower())
            return clean[:15]
        
        district_part = sanitize(district) if district else "all"
        sector_part = sanitize(sector) if sector else "all"
        
        return f"rwanda_boundaries_{district_part}_{sector_part}"

    def _create_table(self, conn, table_name: str):
        """Create PostgreSQL table based on your data structure"""
        drop_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
        
        create_sql = f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            unique_id VARCHAR(36) UNIQUE NOT NULL,
            
            -- Original IDs and codes
            feature_id INTEGER,
            objectid_1 INTEGER,
            code_vill BIGINT,
            code_vill1 BIGINT,
            zones_code VARCHAR(20),
            ea_code VARCHAR(20),
            
            -- Administrative hierarchy
            province_code VARCHAR(10),
            province_name VARCHAR(100),
            province_english VARCHAR(100),
            province_full VARCHAR(200),
            province_id INTEGER,
            
            district_code VARCHAR(10),
            district_name VARCHAR(100),
            district_id INTEGER,
            
            sector_code VARCHAR(10),
            sector_name VARCHAR(100),
            sector_id1 INTEGER,
            sector_id2 INTEGER,
            
            cell_code VARCHAR(20),
            cell_name VARCHAR(100),
            cell_id1 INTEGER,
            cell_id2 INTEGER,
            
            village_name VARCHAR(100),
            village_id INTEGER,
            village_id2 INTEGER,
            
            -- Population and household data
            population INTEGER,
            households INTEGER,
            sum_population INTEGER,
            sum_households INTEGER,
            
            -- Area and shape measurements
            area_km DECIMAL(12,8),
            shape_length DECIMAL(15,8),
            shape_length_1 DECIMAL(15,2),
            shape_length_2 DECIMAL(15,2),
            shape_area DECIMAL(15,2),
            shape_area_1 DECIMAL(15,2),
            
            -- Slope analysis data
            mean_slope DECIMAL(10,6),
            max_slope DECIMAL(10,6),
            min_slope DECIMAL(10,6),
            slope_class VARCHAR(50),
            slope_points_used INTEGER,
            
            -- Urban/Rural classification
            ur_name VARCHAR(100),
            district_council INTEGER,
            status INTEGER,
            
            -- Coordinate system
            coordinates_system VARCHAR(20),
            
            -- Geometry information
            geometry_type VARCHAR(50),
            geometry_geojson JSONB,
            centroid_lat DECIMAL(10,8),
            centroid_lon DECIMAL(11,8),
            
            -- Processing metadata
            processing_metadata JSONB,
            batch_info JSONB,
            
            -- ETL Metadata
            source_collection VARCHAR(200),
            source_database VARCHAR(200),
            extracted_at TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Create indexes for common queries
        index_sql = f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_province ON {table_name}(province_name);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_district ON {table_name}(district_name);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_sector ON {table_name}(sector_name);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_village ON {table_name}(village_name);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_slope ON {table_name}(mean_slope);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_geometry ON {table_name} USING GIN (geometry_geojson);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_centroid ON {table_name}(centroid_lat, centroid_lon);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_population ON {table_name}(population);
        """
        
        conn.execute(text(drop_sql))
        conn.execute(text(create_sql))
        conn.execute(text(index_sql))
        # No need to commit - handled by engine.begin() context manager
        logger.info(f"Table {table_name} created successfully with indexes")

    def _ensure_table_exists(self, conn, table_name: str):
        """Ensure table exists for append mode"""
        try:
            conn.execute(text(f"SELECT 1 FROM {table_name} LIMIT 1;"))
            logger.info(f"Table {table_name} already exists")
        except Exception:
            logger.info(f"Table {table_name} does not exist, creating it")
            self._create_table(conn, table_name)

    def _insert_records(self, conn, table_name: str, records: List[Dict], 
                       update_mode: str) -> int:
        """Insert records into PostgreSQL table"""
        insert_sql = f"""
        INSERT INTO {table_name} (
            unique_id, feature_id, objectid_1, code_vill, code_vill1, zones_code, ea_code,
            province_code, province_name, province_english, province_full, province_id,
            district_code, district_name, district_id,
            sector_code, sector_name, sector_id1, sector_id2,
            cell_code, cell_name, cell_id1, cell_id2,
            village_name, village_id, village_id2,
            population, households, sum_population, sum_households,
            area_km, shape_length, shape_length_1, shape_length_2, shape_area, shape_area_1,
            mean_slope, max_slope, min_slope, slope_class, slope_points_used,
            ur_name, district_council, status, coordinates_system,
            geometry_type, geometry_geojson, centroid_lat, centroid_lon,
            processing_metadata, batch_info,
            source_collection, source_database, extracted_at, created_at
        ) VALUES (
            :unique_id, :feature_id, :objectid_1, :code_vill, :code_vill1, :zones_code, :ea_code,
            :province_code, :province_name, :province_english, :province_full, :province_id,
            :district_code, :district_name, :district_id,
            :sector_code, :sector_name, :sector_id1, :sector_id2,
            :cell_code, :cell_name, :cell_id1, :cell_id2,
            :village_name, :village_id, :village_id2,
            :population, :households, :sum_population, :sum_households,
            :area_km, :shape_length, :shape_length_1, :shape_length_2, :shape_area, :shape_area_1,
            :mean_slope, :max_slope, :min_slope, :slope_class, :slope_points_used,
            :ur_name, :district_council, :status, :coordinates_system,
            :geometry_type, CAST(:geometry_geojson AS JSONB), :centroid_lat, :centroid_lon,
            CAST(:processing_metadata AS JSONB), CAST(:batch_info AS JSONB),
            :source_collection, :source_database, :extracted_at, :created_at
        )
        """
        
        if update_mode == 'append':
            insert_sql += """
            ON CONFLICT (unique_id) DO UPDATE SET
                updated_at = CURRENT_TIMESTAMP
            """
        
        inserted_count = 0
        for record in records:
            try:
                conn.execute(text(insert_sql), record)
                inserted_count += 1
            except Exception as e:
                logger.error(f"Failed to insert record {record.get('unique_id')}: {e}")
                continue
        
        # No need to commit - handled by engine.begin() context manager
        logger.info(f"Committed {inserted_count} records to database")
        return inserted_count

    def get(self, request):
        """Handle GET requests for boundary data extraction"""
        start_time = datetime.now()
        
        try:
            # Get parameters
            district = request.GET.get('district', '').strip()
            sector = request.GET.get('sector', '').strip()
            province = request.GET.get('province', '').strip()
            show_available = request.GET.get('show_available', 'false').lower() == 'true'
            save_to_postgres = request.GET.get('save_to_postgres', 'true').lower() == 'true'
            update_mode = request.GET.get('update_mode', 'replace').lower()
            debug = request.GET.get('debug', 'false').lower() == 'true'
            
            # Validate update mode
            if update_mode not in ['replace', 'append']:
                return JsonResponse({
                    'success': False,
                    'error': 'update_mode must be "replace" or "append"',
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }, status=400)
            
            # Connect to MongoDB
            client = self.connect_mongodb()
            if not client:
                return JsonResponse({
                    'success': False,
                    'error': 'Failed to connect to MongoDB',
                    'connection_details': {
                        'database': self.mongo_db,
                        'collection': self.mongo_collection,
                        'uri_prefix': self.mongo_uri[:50] + "..."
                    },
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }, status=503)
            
            try:
                # Analyze collection structure
                structure = self.analyze_collection_structure(client)
                if not structure['success']:
                    return JsonResponse({
                        'success': False,
                        'error': structure['error'],
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }, status=404)
                
                # Handle debug/discovery mode
                if show_available or debug:
                    return JsonResponse({
                        'success': True,
                        'message': 'Debug mode - Collection analysis complete',
                        'mongodb_connection': {
                            'status': 'connected',
                            'database': self.mongo_db,
                            'collection': self.mongo_collection,
                            'uri_prefix': self.mongo_uri[:50] + "..."
                        },
                        'postgresql_target': {
                            'host': self.pg_config['host'],
                            'port': self.pg_config['port'],
                            'database': self.pg_config['database'],
                            'user': self.pg_config['user']
                        },
                        'collection_analysis': structure,
                        'data_sample': {
                            'has_slope_analysis': structure.get('has_slope_data', False),
                            'has_administrative_data': structure.get('has_admin_data', False),
                            'geometry_type': structure.get('geometry_type'),
                            'total_records': structure.get('total_documents', 0)
                        },
                        'usage_examples': {
                            'all_data': '?save_to_postgres=true&update_mode=replace',
                            'by_district': '?district=Gisagara&save_to_postgres=true',
                            'by_district_sector': '?district=Gisagara&sector=Mugombwa&save_to_postgres=true',
                            'append_mode': '?district=Kigali&update_mode=append&save_to_postgres=true'
                        },
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                
                # Extract data from MongoDB
                documents, extraction_stats = self.extract_filtered_data(
                    client, district, sector, province
                )
                
                if not documents:
                    return JsonResponse({
                        'success': False,
                        'error': 'No documents found matching the criteria',
                        'filters_applied': {
                            'district': district or 'None',
                            'sector': sector or 'None',
                            'province': province or 'None'
                        },
                        'extraction_stats': extraction_stats,
                        'suggestions': {
                            'available_districts': structure.get('sample_districts', [])[:10],
                            'available_sectors': structure.get('sample_sectors', [])[:10],
                            'available_provinces': structure.get('sample_provinces', [])[:5]
                        },
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }, status=404)
                
                # Process documents
                records, processing_stats = self.process_documents(documents)
                
                if not records:
                    return JsonResponse({
                        'success': False,
                        'error': 'Failed to process any documents',
                        'processing_stats': processing_stats,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }, status=500)
                
                # Save to PostgreSQL if requested
                postgres_result = {}
                if save_to_postgres:
                    postgres_result = self.save_to_postgres(records, district, sector, update_mode)
                    
                    # Check if save was successful
                    if not postgres_result.get('success', False):
                        logger.error(f"PostgreSQL save failed: {postgres_result.get('message')}")
                        return JsonResponse({
                            'success': False,
                            'error': 'Failed to save data to PostgreSQL',
                            'details': postgres_result,
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }, status=500)
                
                # Calculate processing time
                processing_time = (datetime.now() - start_time).total_seconds()
                
                return JsonResponse({
                    "success": True,
                    "message": f"Successfully processed and saved {len(records)} boundary records to table '{postgres_result.get('table_name')}'",
                    "table_name": postgres_result.get("table_name"),
                    "records_processed": len(records),
                    "processing_time_seconds": round(processing_time, 2),
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                                    
            finally:
                client.close()
                
        except Exception as e:
            logger.error(f"Unexpected error in boundaries ETL: {e}\n{traceback.format_exc()}")
            return JsonResponse({
                'success': False,
                'error': f'Unexpected error: {str(e)}',
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }, status=500)

    def post(self, request):
        """Handle POST requests from Django forms - WITH REDIRECT"""
        try:
            # Check if it's a form submission or JSON
            content_type = request.META.get('CONTENT_TYPE', '')
            
            # For API requests, return JSON
            if 'application/json' in content_type:
                try:
                    data = json.loads(request.body or "{}")
                except json.JSONDecodeError:
                    data = {}
                
                # Handle as GET for API requests
                class MockGET:
                    def __init__(self, params):
                        self.params = params
                    def get(self, key, default=''):
                        return self.params.get(key, default)
                
                get_params = {
                    'district': data.get('district', ''),
                    'sector': data.get('sector', ''),
                    'province': data.get('province', ''),
                    'show_available': str(data.get('show_available', False)).lower(),
                    'save_to_postgres': str(data.get('save_to_postgres', True)).lower(),
                    'update_mode': data.get('update_mode', 'replace'),
                    'debug': str(data.get('debug', False)).lower()
                }
                
                request.GET = MockGET(get_params)
                return self.get(request)
            
            # For form submissions, redirect with message
            else:
                data = request.POST.dict()
                
                # Get form parameters
                district = data.get('district', '').strip()
                sector = data.get('sector', '').strip()
                province = data.get('province', '').strip()
                update_mode = data.get('update_mode', 'replace')
                
                start_time = datetime.now()
                
                # Connect to MongoDB
                client = self.connect_mongodb()
                if not client:
                    messages.error(request, 'Failed to connect to MongoDB. Please check your connection settings.')
                    return redirect('/etl/')  # Redirect to ETL main page
                
                try:
                    # Analyze collection
                    structure = self.analyze_collection_structure(client)
                    if not structure['success']:
                        messages.error(request, f"Collection analysis failed: {structure['error']}")
                        return redirect('/etl/')
                    
                    # Extract data
                    documents, extraction_stats = self.extract_filtered_data(
                        client, district, sector, province
                    )
                    
                    if not documents:
                        messages.warning(
                            request, 
                            f'No documents found for District: {district or "All"}, Sector: {sector or "All"}. '
                            f'Please check your filters.'
                        )
                        return redirect('/etl/')
                    
                    # Process documents
                    records, processing_stats = self.process_documents(documents)
                    
                    if not records:
                        messages.error(request, 'Failed to process any documents. Please check the data format.')
                        return redirect('/etl/')
                    
                    # Save to PostgreSQL
                    postgres_result = self.save_to_postgres(records, district, sector, update_mode)
                    
                    if not postgres_result.get('success', False):
                        messages.error(
                            request, 
                            f"Failed to save data to PostgreSQL: {postgres_result.get('message', 'Unknown error')}"
                        )
                        return redirect('/etl/')
                    
                    # Calculate processing time
                    processing_time = (datetime.now() - start_time).total_seconds()
                    
                    # Success message
                    messages.success(
                        request,
                        f'✅ Successfully saved {len(records)} boundary records to table '
                        f'"{postgres_result.get("table_name")}" in {processing_time:.2f} seconds!'
                    )
                    
                    return redirect('/etl/')
                    
                finally:
                    client.close()
            
        except Exception as e:
            logger.error(f"POST request error: {e}\n{traceback.format_exc()}")
            messages.error(request, f'An unexpected error occurred: {str(e)}')
            return redirect('/etl/')
