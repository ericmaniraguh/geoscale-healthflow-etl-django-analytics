# etl_app/services/postgresql_service.py - FULLY FIXED VERSION
"""Fixed PostgreSQL operations service with proper analytics table creation"""

import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text, inspect
from django.conf import settings
from typing import List, Dict, Any, Tuple, Optional
import traceback
import json
from ..utils.helpers import generate_dynamic_table_name         

logger = logging.getLogger(__name__)


class PostgreSQLService:
    """Fixed PostgreSQL service with proper analytics table handling"""
    
    def __init__(self):
        self._engine = None
        self._test_connection()
    
    def _test_connection(self):
        """Test database connection on initialization"""
        try:
            engine = self._create_engine()
            if engine:
                with engine.begin() as conn:
                    conn.execute(text("SELECT 1"))
                    logger.info("PostgreSQL connection test successful")
                    return True
            return False
        except Exception as e:
            logger.error(f"PostgreSQL connection test failed: {str(e)}")
            return False
        
    from datetime import datetime

    def format_timestamp(dt):
        return dt.strftime("%Y-%m-%d %H:%M")

    
    def _create_engine(self):
        """Create SQLAlchemy engine with proper configuration"""
        try:
            db_config = settings.DATABASES['default']
            
            connection_string = (
                f"postgresql://{db_config['USER']}:{db_config['PASSWORD']}@"
                f"{db_config['HOST']}:{db_config['PORT']}/{db_config['NAME']}"
            )
            
            engine = create_engine(
                connection_string,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=3600,
                echo=False
            )
            
            logger.info(f"PostgreSQL engine created: {db_config['HOST']}:{db_config['PORT']}/{db_config['NAME']}")
            return engine
            
        except Exception as e:
            logger.error(f"Failed to create PostgreSQL engine: {str(e)}")
            return None
    
    @property
    def engine(self):
        """Get database engine with connection validation"""
        if not self._engine:
            self._engine = self._create_engine()
        
        try:
            if self._engine:
                with self._engine.begin() as conn:
                    conn.execute(text("SELECT 1"))
        except Exception:
            logger.warning("Engine connection unhealthy, recreating...")
            self._engine = self._create_engine()
        
        return self._engine

    def save_raw_data(self, data: List[Dict[str, Any]], table_name: str = "health_center_lab_data", 
                update_mode: str = 'replace', district: Optional[str] = None, 
                sector: Optional[str] = None) -> Tuple[bool, str]:
            """Save raw data with STATIC table naming - no years in table name"""
            if not data:
                return False, "No data to save"

            if not self.engine:
                return False, "Cannot connect to PostgreSQL"

            # Generate STATIC table name WITHOUT extracting years
            static_table_name = generate_dynamic_table_name(
                base_name=table_name, 
                district=district, 
                sector=sector, 
                years_covered=None  # Explicitly set to None for static naming
            )

            logger.info(f"STARTING: Save {len(data)} records to STATIC table '{static_table_name}' (mode: {update_mode})")
            logger.info(f"STATIC NAMING: No years extracted, table name is consistent")

            try:
                with self.engine.begin() as conn:
                    
                    if update_mode == 'replace':
                        drop_sql = f"DROP TABLE IF EXISTS {static_table_name} CASCADE"  # FIXED: was dynamic_table_name
                        conn.execute(text(drop_sql))
                        logger.info(f"DROPPED: Existing table {static_table_name}")
                    
                    create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {static_table_name} (  -- FIXED: was dynamic_table_name
                        id SERIAL PRIMARY KEY,
                        unique_id VARCHAR(36) UNIQUE NOT NULL,
                        year INTEGER,
                        month INTEGER,
                        district VARCHAR(100),
                        sector VARCHAR(100),
                        health_center VARCHAR(200),
                        cell VARCHAR(100),
                        village VARCHAR(100),
                        age INTEGER,
                        age_group VARCHAR(20),
                        gender VARCHAR(20),
                        slide_status TEXT,
                        test_result VARCHAR(20),
                        is_positive BOOLEAN DEFAULT FALSE,
                        case_origin VARCHAR(100),
                        province VARCHAR(100),
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW()
                        
                    )
                    """
                    conn.execute(text(create_sql))
                    logger.info(f"CREATED: Table {static_table_name}")
                    
                    # Insert data
                    records_inserted = 0
                    records_updated = 0
                    errors = []
                    
                    for i, record in enumerate(data):
                        try:
                            safe_record = {
                                'unique_id': record.get('unique_id') or f'auto_{i}_{int(datetime.now().timestamp())}',
                                'year': self._safe_int(record.get('year')),
                                'month': self._safe_int(record.get('month')),
                                'district': self._safe_string(record.get('district'), 100),
                                'sector': self._safe_string(record.get('sector'), 100),
                                'health_center': self._safe_string(record.get('health_center'), 200),
                                'cell': self._safe_string(record.get('cell'), 100),
                                'village': self._safe_string(record.get('village'), 100),
                                'age': self._safe_int(record.get('age')),
                                'age_group': self._safe_string(record.get('age_group'), 20),
                                'gender': self._safe_string(record.get('gender'), 20),
                                'slide_status': self._safe_string(record.get('slide_status')),
                                'test_result': self._safe_string(record.get('test_result'), 20),
                                'is_positive': bool(record.get('is_positive', False)),
                                'case_origin': self._safe_string(record.get('case_origin'), 100),
                                'province': self._safe_string(record.get('province'), 100),
                                'created_at': record.get('created_at') or datetime.now().strftime('%Y-%m-%d %H:%M')
                            }
                            
                            if update_mode == 'append':
                                upsert_sql = f"""
                                INSERT INTO {static_table_name}   -- FIXED: was dynamic_table_name
                                (unique_id, year, month, district, sector, health_center, cell, village,
                                    age, age_group, gender, slide_status, test_result, is_positive,
                                    case_origin, province, create_at)
                                VALUES (:unique_id, :year, :month, :district, :sector, :health_center, :cell, :village,
                                        :age, :age_group, :gender, :slide_status, :test_result, :is_positive,
                                        :case_origin, :province, :created_at)
                                ON CONFLICT (unique_id) DO UPDATE SET
                                    year = EXCLUDED.year,
                                    month = EXCLUDED.month,
                                    district = EXCLUDED.district,
                                    sector = EXCLUDED.sector,
                                    health_center = EXCLUDED.health_center,
                                    cell = EXCLUDED.cell,
                                    village = EXCLUDED.village,
                                    age = EXCLUDED.age,
                                    age_group = EXCLUDED.age_group,
                                    gender = EXCLUDED.gender,
                                    slide_status = EXCLUDED.slide_status,
                                    test_result = EXCLUDED.test_result,
                                    is_positive = EXCLUDED.is_positive,
                                    case_origin = EXCLUDED.case_origin,
                                    province = EXCLUDED.province,
                                    updated_at = NOW()
                                RETURNING (xmax = 0) AS inserted
                                """
                                
                                result = conn.execute(text(upsert_sql), safe_record)
                                row = result.fetchone()
                                if row and row[0]:
                                    records_inserted += 1
                                else:
                                    records_updated += 1
                            else:
                                insert_sql = f"""
                                INSERT INTO {static_table_name}   -- FIXED: was dynamic_table_name
                                (unique_id, year, month, district, sector, health_center, cell, village,
                                    age, age_group, gender, slide_status, test_result, is_positive,
                                    case_origin, province, created_at)
                                VALUES (:unique_id, :year, :month, :district, :sector, :health_center, :cell, :village,
                                        :age, :age_group, :gender, :slide_status, :test_result, :is_positive,
                                        :case_origin, :province, :created_at)
                                """
                                
                                conn.execute(text(insert_sql), safe_record)
                                records_inserted += 1
                            
                            if (i + 1) % 100 == 0:
                                logger.info(f"PROGRESS: Processed {i + 1}/{len(data)} records")
                            
                        except Exception as record_error:
                            error_msg = f"Record {i}: {str(record_error)}"
                            errors.append(error_msg)
                            logger.error(f"RECORD ERROR: {error_msg}")
                            continue
                    
                    count_sql = f"SELECT COUNT(*) FROM {static_table_name}"  # FIXED: was dynamic_table_name
                    result = conn.execute(text(count_sql))
                    final_count = result.fetchone()[0]
                    
                    logger.info(f"SUCCESS: {static_table_name} now contains {final_count} total records")
                    
                    message = f"Successfully processed {len(data)} records to '{static_table_name}'. Final count: {final_count}"
                    if update_mode == 'append':
                        message += f" (inserted: {records_inserted}, updated: {records_updated})"
                    if errors:
                        message += f". Errors: {len(errors)}"
                    
                    return True, message
                    
            except Exception as e:
                logger.error(f"SAVE ERROR: {str(e)}")
                logger.error(f"TRACEBACK: {traceback.format_exc()}")
                return False, f"Database save failed: {str(e)}"
    def save_analytics(self, analytics_data: Dict[str, Any], table_prefix: str = "analytics",
                  district: Optional[str] = None, sector: Optional[str] = None,
                  years: Optional[List[int]] = None, update_mode: str = 'replace') -> Tuple[bool, Dict[str, Any]]:
            """STATIC: Save all analytics with static table naming - no years in names"""
            if not self.engine:
                return False, {"error": "Cannot connect to PostgreSQL"}
            
            logger.info(f"ANALYTICS SAVE: Starting with prefix '{table_prefix}' - STATIC NAMING")
            logger.info(f"ANALYTICS SAVE: Analytics data types: {list(analytics_data.keys())}")
            
            results = {}
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
            
            try:
                with self.engine.begin() as conn:
                    
                    for analytics_type, data in analytics_data.items():
                        if not data:
                            logger.warning(f"ANALYTICS: {analytics_type} has no data, skipping")
                            results[analytics_type] = "SKIPPED: No data"
                            continue
                        
                        try:
                            logger.info(f"ANALYTICS: Processing {analytics_type} with {len(data) if isinstance(data, list) else 1} records")
                            
                            # Generate STATIC table name - NO years extraction
                            if analytics_type == 'yearly_slide_status':
                                table_name = generate_dynamic_table_name(f"{table_prefix}_yearly_statistics", district, sector, None)
                                schema_type = 'yearly_stats'
                            elif analytics_type == 'gender_positivity_by_year':
                                table_name = generate_dynamic_table_name(f"{table_prefix}_gender_pos_by_year", district, sector, None)
                                schema_type = 'gender_pos'
                            elif analytics_type == 'village_positivity_by_year':
                                table_name = generate_dynamic_table_name(f"{table_prefix}_village_pos_by_year", district, sector, None)
                                schema_type = 'village_pos'
                            elif analytics_type == 'total_summary':
                                table_name = generate_dynamic_table_name(f"{table_prefix}_total_summary", district, sector, None)
                                schema_type = 'summary'
                            elif analytics_type == 'monthly_positivity':
                                table_name = generate_dynamic_table_name(f"{table_prefix}_monthly_positivity", district, sector, None)
                                schema_type = 'monthly_pos'
                            else:
                                logger.warning(f"ANALYTICS: Unknown analytics type: {analytics_type}")
                                results[analytics_type] = f"ERROR: Unknown type {analytics_type}"
                                continue
                            
                            logger.info(f"ANALYTICS: STATIC table name for {analytics_type}: {table_name}")
                            
                            # Save this analytics table with static naming
                            success = self._save_analytics_table_fixed(
                                conn, table_name, data, schema_type, update_mode, 
                                district, sector, [], timestamp, analytics_type  # Empty years list
                            )
                            
                            if success:
                                results[analytics_type] = f"Saved to {table_name}"
                                logger.info(f"ANALYTICS SUCCESS: {analytics_type} -> {table_name}")
                            else:
                                results[analytics_type] = f"Failed: {table_name}"
                                logger.error(f"ANALYTICS FAILED: {analytics_type} -> {table_name}")
                            
                        except Exception as analytics_error:
                            error_msg = f"ERROR: {str(analytics_error)}"
                            results[analytics_type] = error_msg
                            logger.error(f"ANALYTICS ERROR for {analytics_type}: {str(analytics_error)}")
                            continue
                    
                    logger.info("ANALYTICS SAVE: All analytics processed with STATIC naming")
                    return True, results

            except Exception as e:
                logger.error(f"ANALYTICS SAVE TRANSACTION ERROR: {str(e)}")
                return False, {"error": str(e), "results": results}


    # For API East data specifically - add this mapping
    def save_api_data(self, data: List[Dict[str, Any]], district: Optional[str] = None, 
                        sector: Optional[str] = None, update_mode: str = 'replace') -> Tuple[bool, str]:
            """Save API East data with static naming"""
            return self.save_raw_data(
                data=data,
                table_name="api_data",  # This will map to "api"
                district=district,
                sector=sector,
                update_mode=update_mode
            )

    def _extract_years_covered(self, data, analytics_type: str) -> List[int]:
        """Extract years_covered from analytics data and convert to list of integers"""
        years_covered_list = []
        
        try:
            # Handle different data structures
            if isinstance(data, list) and len(data) > 0:
                # For list data, check first record
                first_record = data[0]
                if isinstance(first_record, dict) and 'years_covered' in first_record:
                    years_covered = first_record['years_covered']
                else:
                    # Fallback: extract years from year field in records
                    years_set = set()
                    for record in data:
                        if isinstance(record, dict) and 'year' in record:
                            years_set.add(record['year'])
                    years_covered_list = sorted(list(years_set))
                    logger.info(f"YEARS EXTRACTION: Fallback years from records: {years_covered_list}")
                    return years_covered_list
            
            elif isinstance(data, dict):
                # For single record/dict data
                if 'years_covered' in data:
                    years_covered = data['years_covered']
                else:
                    # Fallback for single record
                    if 'year' in data:
                        years_covered_list = [data['year']]
                    logger.info(f"YEARS EXTRACTION: Single record fallback: {years_covered_list}")
                    return years_covered_list
            
            else:
                logger.warning(f"YEARS EXTRACTION: Unexpected data structure for {analytics_type}")
                return []
            
            # Process years_covered field
            if isinstance(years_covered, str):
                # Handle string representation like '[2021, 2022, 2023]'
                if years_covered.startswith('[') and years_covered.endswith(']'):
                    # Remove brackets and split
                    years_str = years_covered.strip('[]')
                    years_covered_list = [int(year.strip()) for year in years_str.split(',') if year.strip().isdigit()]
                else:
                    # Handle comma-separated string like '2021,2022,2023'
                    years_covered_list = [int(year.strip()) for year in years_covered.split(',') if year.strip().isdigit()]
            
            elif isinstance(years_covered, list):
                # Handle list of integers or strings
                years_covered_list = [int(year) for year in years_covered if str(year).isdigit()]
            
            elif isinstance(years_covered, (int, float)):
                # Handle single year
                years_covered_list = [int(years_covered)]
            
            else:
                logger.warning(f"YEARS EXTRACTION: Unknown years_covered format: {type(years_covered)} - {years_covered}")
                return []
            
            # Sort and remove duplicates
            years_covered_list = sorted(list(set(years_covered_list)))
            logger.info(f"YEARS EXTRACTION: Successfully extracted {years_covered_list} from {analytics_type}")
            return years_covered_list
            
        except Exception as e:
            logger.error(f"YEARS EXTRACTION ERROR for {analytics_type}: {str(e)}")
            logger.error(f"YEARS EXTRACTION: Data sample: {str(data)[:200]}...")
            return []

    # def _save_analytics_table_fixed(self, conn, table_name: str, data, 
    #                                schema_type: str, update_mode: str, district: str, 
    #                                sector: str, years: List[int], timestamp: str, analytics_type: str) -> bool:
    #     """FIXED: Save individual analytics table with proper data handling"""
    #     try:
    #         logger.info(f"ANALYTICS TABLE: Creating {table_name} (type: {schema_type})")
            
    #         # Drop table if replace mode
    #         if update_mode == 'replace':
    #             conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
    #             logger.info(f"DROPPED: Table {table_name}")
            
    #         # Create table with appropriate schema
    #         schema = self._get_schema_by_type(table_name, schema_type)
    #         conn.execute(text(schema))
    #         logger.info(f"CREATED: Table {table_name}")
            
    #         # Convert single record to list if needed
    #         if not isinstance(data, list):
    #             data_list = [data]
    #         else:
    #             data_list = data
            
    #         logger.info(f"PROCESSING: {len(data_list)} records for {table_name}")
            
    #         # Process each record
    #         inserted = 0
    #         for i, record in enumerate(data_list):
    #             try:
    #                 # Create a clean copy of the record
    #                 clean_record = {}
                    
    #                 # Add all fields from the original record, cleaning as needed
    #                 for key, value in record.items():
    #                     clean_record[key] = self._clean_value_for_db(value, key)
                    
    #                 # Add metadata fields
    #                 # clean_record['calculated_at'] = timestamp
    #                 clean_record['filter_district'] = district or 'all'
    #                 clean_record['filter_sector'] = sector or 'all'
    #                 clean_record['filter_years'] = ','.join(map(str, sorted(years))) if years else 'all'
                    
    #                 # Handle special conversions for summary type
    #                 if schema_type == 'summary':
    #                     for field in ['years_covered', 'districts_covered', 'sectors_covered', 'gender_breakdown', 'age_group_breakdown']:
    #                         if field in clean_record and clean_record[field] is not None:
    #                             if isinstance(clean_record[field], (list, dict)):
    #                                 clean_record[field] = json.dumps(clean_record[field])
    #                             else:
    #                                 clean_record[field] = str(clean_record[field])
                    
    #                 # Build dynamic insert statement
    #                 columns = list(clean_record.keys())
    #                 placeholders = [f":{col}" for col in columns]
                    
    #                 insert_sql = f"""
    #                 INSERT INTO {table_name} ({', '.join(columns)})
    #                 VALUES ({', '.join(placeholders)})
    #                 """
                    
    #                 # Execute insert with detailed error logging
    #                 try:
    #                     conn.execute(text(insert_sql), clean_record)
    #                     inserted += 1
                        
    #                     if (i + 1) % 10 == 0:
    #                         logger.info(f"ANALYTICS PROGRESS: {i + 1}/{len(data_list)} records inserted")
                            
    #                 except Exception as insert_error:
    #                     logger.error(f"ANALYTICS INSERT ERROR for record {i}: {str(insert_error)}")
    #                     logger.error(f"ANALYTICS RECORD DATA: {clean_record}")
    #                     logger.error(f"ANALYTICS INSERT SQL: {insert_sql}")
    #                     continue
                        
    #             except Exception as record_error:
    #                 logger.error(f"ANALYTICS RECORD PROCESSING ERROR {i}: {str(record_error)}")
    #                 logger.error(f"ANALYTICS RAW RECORD: {record}")
    #                 continue
            
    #         # Verify final count
    #         count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
    #         final_count = count_result.fetchone()[0]
            
    #         logger.info(f"ANALYTICS SUCCESS: {table_name} - {final_count} total records ({inserted} inserted)")
            
    #         # Show sample data for verification
    #         if final_count > 0:
    #             sample_sql = f"SELECT * FROM {table_name} LIMIT 2"
    #             result = conn.execute(text(sample_sql))
    #             samples = result.fetchall()
    #             logger.info(f"ANALYTICS SAMPLE DATA: {[dict(row._mapping) for row in samples]}")
            
    #         return inserted > 0
            
    #     except Exception as e:
    #         logger.error(f"ANALYTICS TABLE ERROR {table_name}: {str(e)}")
    #         logger.error(f"ANALYTICS TABLE TRACEBACK: {traceback.format_exc()}")
    #         return False
    def _save_analytics_table_fixed(self, conn, table_name: str, data, 
                               schema_type: str, update_mode: str, district: str, 
                               sector: str, years: List[int], timestamp: str, analytics_type: str) -> bool:
        """FIXED: Save individual analytics table with proper transaction rollback"""
        
        # Start an explicit transaction
        trans = conn.begin()
        
        try:
            logger.info(f"ANALYTICS TABLE: Creating {table_name} (type: {schema_type})")
            
            # Drop table if replace mode
            if update_mode == 'replace':
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                logger.info(f"DROPPED: Table {table_name}")
            
            # Create table with appropriate schema
            schema = self._get_schema_by_type(table_name, schema_type)
            conn.execute(text(schema))
            logger.info(f"CREATED: Table {table_name}")
            
            # Convert single record to list if needed
            if not isinstance(data, list):
                data_list = [data]
            else:
                data_list = data
            
            logger.info(f"PROCESSING: {len(data_list)} records for {table_name}")
            
            # Process each record
            inserted = 0
            failed = 0
            
            for i, record in enumerate(data_list):
                try:
                    # Create a clean copy of the record
                    clean_record = {}
                    
                    # Add all fields from the original record, cleaning as needed
                    for key, value in record.items():
                        clean_record[key] = self._clean_value_for_db(value, key)
                    
                    # Add metadata fields
                    clean_record['filter_district'] = district or 'all'
                    clean_record['filter_sector'] = sector or 'all'
                    clean_record['filter_years'] = ','.join(map(str, sorted(years))) if years else 'all'
                    
                    # Handle special conversions for summary type
                    if schema_type == 'summary':
                        for field in ['years_covered', 'districts_covered', 'sectors_covered', 'gender_breakdown', 'age_group_breakdown']:
                            if field in clean_record and clean_record[field] is not None:
                                if isinstance(clean_record[field], (list, dict)):
                                    clean_record[field] = json.dumps(clean_record[field])
                                else:
                                    clean_record[field] = str(clean_record[field])
                    
                    # Build dynamic insert statement
                    columns = list(clean_record.keys())
                    placeholders = [f":{col}" for col in columns]
                    
                    insert_sql = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                    """
                    
                    # Execute insert
                    conn.execute(text(insert_sql), clean_record)
                    inserted += 1
                    
                    if (i + 1) % 10 == 0:
                        logger.info(f"ANALYTICS PROGRESS: {i + 1}/{len(data_list)} records inserted")
                        
                except Exception as insert_error:
                    failed += 1
                    logger.error(f"ANALYTICS INSERT ERROR for record {i}: {str(insert_error)}")
                    logger.error(f"ANALYTICS RECORD DATA: {clean_record}")
                    
                    # If too many failures, abort completely
                    if failed > 10:
                        logger.error(f"Too many insert failures ({failed}), aborting transaction")
                        trans.rollback()  # ✅ Rollback on too many errors
                        return False
                    
                    # For individual errors, log but continue
                    continue
            
            # Commit the transaction if we have successful inserts
            if inserted > 0:
                trans.commit()  # ✅ Commit successful inserts
                logger.info(f"TRANSACTION COMMITTED: {inserted} records inserted")
            else:
                trans.rollback()  # ✅ Rollback if nothing inserted
                logger.warning(f"NO RECORDS INSERTED: Rolling back transaction")
                return False
            
            # Verify final count (in a new implicit transaction)
            count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            final_count = count_result.fetchone()[0]
            
            logger.info(f"ANALYTICS SUCCESS: {table_name} - {final_count} total records ({inserted} inserted, {failed} failed)")
            
            # Show sample data for verification
            if final_count > 0:
                sample_sql = f"SELECT * FROM {table_name} LIMIT 2"
                result = conn.execute(text(sample_sql))
                samples = result.fetchall()
                logger.info(f"ANALYTICS SAMPLE DATA: {[dict(row._mapping) for row in samples]}")
            
            return inserted > 0
            
        except Exception as e:
            # Rollback on any major error
            trans.rollback()  # ✅ Always rollback on exception
            logger.error(f"ANALYTICS TABLE ERROR {table_name}: {str(e)}")
            logger.error(f"ANALYTICS TABLE TRACEBACK: {traceback.format_exc()}")
            return False
    
    def _clean_value_for_db(self, value, field_name: str):
        """Clean and convert values for database insertion"""
        if value is None:
            return None
        
        # Handle numpy scalars
        if hasattr(value, 'item'):  # e.g. numpy scalar
            try:
                return value.item()
            except Exception:
                pass
        
        import numpy as np
        
        # Convert numpy types to native Python
        if isinstance(value, np.integer):
            return int(value)
        elif isinstance(value, np.floating):
            return float(value)
        elif isinstance(value, np.bool_):
            return bool(value)
        elif isinstance(value, (np.str_, str)):   # <-- FIX: handle np.str_ and plain str
            value = str(value)
        
        # Handle JSON-like types
        if isinstance(value, (list, dict)):
            return json.dumps(value) if field_name in [
                'years_covered', 'districts_covered', 'sectors_covered',
                'gender_breakdown', 'age_group_breakdown'
            ] else str(value)
        
        # Handle string length limits
        if isinstance(value, str):
            if field_name in ['district', 'sector', 'filter_district', 'filter_sector']:
                return value[:100]
            elif field_name in ['gender', 'month_name', 'age_group']:
                return value[:20]
            elif field_name == 'village':
                return value[:100]
            elif field_name == 'unique_id':
                return value[:36]
        
        return value

    def _get_schema_by_type(self, table_name: str, schema_type: str) -> str:
        """Get schema SQL based on analytics type"""
        if schema_type == 'yearly_stats':
            return self._get_yearly_stats_schema(table_name)
        elif schema_type == 'gender_pos':
            return self._get_gender_pos_schema(table_name)
        elif schema_type == 'village_pos':
            return self._get_village_pos_schema(table_name)
        elif schema_type == 'summary':
            return self._get_summary_schema(table_name)
        elif schema_type == 'monthly_pos':
            return self._get_monthly_pos_schema(table_name)
        else:
            raise ValueError(f"Unknown schema type: {schema_type}")
        
    def _get_yearly_stats_schema(self, table_name: str) -> str:
        """Schema for yearly slide status statistics"""
        schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            unique_id VARCHAR(36) UNIQUE NOT NULL,
            year INTEGER NOT NULL,
            total_tests INTEGER DEFAULT 0,
            positive_cases INTEGER DEFAULT 0,
            negative_cases INTEGER DEFAULT 0,
            inconclusive_cases INTEGER DEFAULT 0,
            positivity_rate DECIMAL(5,2) DEFAULT 0,
            negativity_rate DECIMAL(5,2) DEFAULT 0,
            inconclusive_rate DECIMAL(5,2) DEFAULT 0,
            filter_district VARCHAR(100),
            filter_sector VARCHAR(100),
            filter_years VARCHAR(100),
              created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_{table_name.replace('-', '_')}_year ON {table_name}(year);
        """
        logger.info(f"YEARLY STATS SCHEMA for {table_name}:\n{schema}")
        return schema

    def _get_gender_pos_schema(self, table_name: str) -> str:
        """Schema for gender positivity by year"""
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            unique_id VARCHAR(36) UNIQUE NOT NULL,
            year INTEGER NOT NULL,
            gender VARCHAR(20) NOT NULL,
            total_tests INTEGER DEFAULT 0,
            positive_cases INTEGER DEFAULT 0,
            negative_cases INTEGER DEFAULT 0,
            inconclusive_cases INTEGER DEFAULT 0,
            positivity_rate DECIMAL(5,2) DEFAULT 0,
            negativity_rate DECIMAL(5,2) DEFAULT 0,
            inconclusive_rate DECIMAL(5,2) DEFAULT 0,
            filter_district VARCHAR(100),
            filter_sector VARCHAR(100),
            filter_years VARCHAR(100),
                created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_{table_name.replace('-', '_')}_year_gender ON {table_name}(year, gender);
        """

    def _get_village_pos_schema(self, table_name: str) -> str:
        """Schema for village positivity by year"""
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            unique_id VARCHAR(36) UNIQUE NOT NULL,
            village VARCHAR(100) NOT NULL,
            year INTEGER NOT NULL,
            district VARCHAR(100),
            sector VARCHAR(100),
            total_tests INTEGER DEFAULT 0,
            positive_cases INTEGER DEFAULT 0,
            negative_cases INTEGER DEFAULT 0,
            positivity_rate DECIMAL(5,2) DEFAULT 0,
            filter_district VARCHAR(100),
            filter_sector VARCHAR(100),
            filter_years VARCHAR(100),
                created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_{table_name.replace('-', '_')}_village_year ON {table_name}(village, year);
        """

    def _get_summary_schema(self, table_name: str) -> str:
        """Schema for total summary statistics"""
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            unique_id VARCHAR(36) UNIQUE NOT NULL,
            total_records INTEGER DEFAULT 0,
            total_positive_cases INTEGER DEFAULT 0,
            total_negative_cases INTEGER DEFAULT 0,
            total_inconclusive_cases INTEGER DEFAULT 0,
            overall_pos_rate DECIMAL(5,2) DEFAULT 0,
            year_range VARCHAR(50),
            years_covered TEXT,
            districts_count INTEGER DEFAULT 0,
            sectors_count INTEGER DEFAULT 0,
            villages_count INTEGER DEFAULT 0,
            districts_covered TEXT,
            sectors_covered TEXT,
            gender_breakdown TEXT,
            age_group_breakdown TEXT,
            filter_district VARCHAR(100),
            filter_sector VARCHAR(100),
            filter_years VARCHAR(100),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        """

    def _get_monthly_pos_schema(self, table_name: str) -> str:
        """Schema for monthly positivity rates"""
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            unique_id VARCHAR(36) UNIQUE NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            month_name VARCHAR(20),
            total_tests INTEGER DEFAULT 0,
            positive_cases INTEGER DEFAULT 0,
            positivity_rate DECIMAL(5,2) DEFAULT 0,
            filter_district VARCHAR(100),
            filter_sector VARCHAR(100),
            filter_years VARCHAR(100),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_{table_name.replace('-', '_')}_year_month ON {table_name}(year, month);
        """
    
    # Utility methods for safe data handling
    def _safe_string(self, value, max_length=None):
        """Safely convert value to string with length limit"""
        if value is None:
            return ''
        string_val = str(value).strip()
        if max_length:
            string_val = string_val[:max_length]
        return string_val
    
    def _safe_int(self, value):
        """Safely convert value to integer"""
        if value is None or value == '':
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value):
        """Safely convert value to float"""
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def verify_table_data(self, table_name: str) -> Dict[str, Any]:
        """Verify table exists and has data"""
        if not self.engine:
            return {'exists': False, 'error': 'No database connection'}
        
        try:
            with self.engine.begin() as conn:
                exists_sql = f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                )
                """
                result = conn.execute(text(exists_sql))
                exists = result.fetchone()[0]
                
                if not exists:
                    return {'exists': False, 'count': 0}
                
                count_sql = f"SELECT COUNT(*) FROM {table_name}"
                result = conn.execute(text(count_sql))
                count = result.fetchone()[0]
                
                sample_sql = f"SELECT * FROM {table_name} LIMIT 3"
                result = conn.execute(text(sample_sql))
                samples = [dict(row._mapping) for row in result.fetchall()]
                
                return {
                    'exists': True,
                    'count': count,
                    'samples': samples
                }
                
        except Exception as e:
            logger.error(f"TABLE VERIFICATION ERROR: {str(e)}")
            return {'exists': False, 'error': str(e)}

    def save_boundaries_data(self, records: List[Dict[str, Any]], table_name: str = "rwanda_boundaries_all", 
                           update_mode: str = "upsert") -> Tuple[bool, Dict[str, Any]]:
        """Save or update administrative boundaries data"""
        if not records:
            return False, {"message": "No boundary records to save"}

        if not self.engine:
            return False, {"message": "Cannot connect to PostgreSQL"}

        try:
            with self.engine.begin() as conn:
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    unique_id VARCHAR(36) NOT NULL,
                    province_name VARCHAR(100),
                    district_name VARCHAR(100),
                    sector_name VARCHAR(100),
                    cell_name VARCHAR(100),
                    village_name VARCHAR(100),
                    population INTEGER,
                    households INTEGER,
                    mean_slope DECIMAL(10,6),
                    geometry_type VARCHAR(50),
                    geometry_geojson JSONB,
                    centroid_lat DECIMAL(10,8),
                    centroid_lon DECIMAL(11,8),
                    UNIQUE (province_name, district_name, sector_name, village_name),
                        created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
                );
                """
                conn.execute(text(create_sql))

                upsert_sql = f"""
                INSERT INTO {table_name} (
                    unique_id, province_name, district_name, sector_name,
                    cell_name, village_name, population, households,
                    mean_slope, geometry_type, geometry_geojson,
                    centroid_lat, centroid_lon, created_at, updated_at
                )
                VALUES (
                    :unique_id, :province_name, :district_name, :sector_name,
                    :cell_name, :village_name, :population, :households,
                    :mean_slope, :geometry_type, CAST(:geometry_geojson AS JSONB),
                    :centroid_lat, :centroid_lon, NOW(), NOW()
                )
                ON CONFLICT (province_name, district_name, sector_name, village_name)
                DO UPDATE SET
                    unique_id = EXCLUDED.unique_id,
                    cell_name = EXCLUDED.cell_name,
                    population = EXCLUDED.population,
                    households = EXCLUDED.households,
                    mean_slope = EXCLUDED.mean_slope,
                    geometry_type = EXCLUDED.geometry_type,
                    geometry_geojson = EXCLUDED.geometry_geojson,
                    centroid_lat = EXCLUDED.centroid_lat,
                    centroid_lon = EXCLUDED.centroid_lon,
                    updated_at = NOW()
                RETURNING (xmax = 0) AS inserted;
                """

                inserted, updated = 0, 0
                for rec in records:
                    result = conn.execute(text(upsert_sql), rec)
                    row = result.fetchone()
                    if row and row[0]:  # True if insert
                        inserted += 1
                    else:
                        updated += 1

            return True, {
                    "message": f"Boundary data saved. Inserted {inserted}, Updated {updated}",
                    "table_name": table_name,
                    "inserted": inserted,
                    "updated": updated,
                    "processed": inserted + updated
                }

        except Exception as e:
            logger.error(f"BOUNDARIES UPSERT ERROR: {e}")
            return False, {"message": f"Failed to save boundaries: {str(e)}"}