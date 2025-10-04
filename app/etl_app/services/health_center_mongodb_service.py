# etl_app/services/health_center_mongodb_service.py - SIMPLIFIED SINGLE COLLECTION APPROACH
"""MongoDB service for health center data using single collection with document filtering"""

import logging
from pymongo import MongoClient
from django.conf import settings
from typing import List, Dict, Any, Optional
import time
import traceback

logger = logging.getLogger(__name__)

class HealthCenterMongoDBService:
    """MongoDB service for health center lab data using single collection"""
    
    def __init__(self):
        self.client = None
        self.mongo_uri = getattr(settings, 'MONGO_URI', 'mongodb://localhost:27017/')
        self.mongo_db = getattr(settings, 'MONGO_DB', 'malaria-lab-records-db')
        self.collection_name = getattr(settings, 'MONGO_COLLECTION', 'healthcenter-data')
        logger.info(f"HEALTH CENTER: Initializing with DB: {self.mongo_db}, Collection: {self.collection_name}")
        
    def _connect(self):
        """Connect to health center MongoDB database"""
        if not self.client:
            try:
                self.client = MongoClient(
                    self.mongo_uri, 
                    serverSelectionTimeoutMS=10000,
                    maxPoolSize=50,
                    waitQueueTimeoutMS=5000
                )
                self.client.admin.command('ismaster')
                logger.info(f"HEALTH CENTER: Connected to MongoDB at {self.mongo_uri}")
            except Exception as e:
                logger.error(f"HEALTH CENTER: MongoDB connection failed: {str(e)}")
                self.client = None
                raise
        return self.client
    
    def get_available_filters(self) -> Dict[str, List]:
        """Get available filters from health center collection"""
        try:
            client = self._connect()
            db = client[self.mongo_db]
            
            # Find the actual collection that exists
            all_collections = db.list_collection_names()
            hc_collections = [col for col in all_collections if 'healthcenter-data' in col and not col.endswith('_metadata')]
            
            logger.info(f"HEALTH CENTER: Found collections: {hc_collections}")
            
            if not hc_collections:
                logger.warning("HEALTH CENTER: No health center collections found")
                return {'years': [], 'districts': [], 'sectors': []}
            
            all_years = set()
            all_districts = set()
            all_sectors = set()
            
            # Check each health center collection
            for collection_name in hc_collections:
                try:
                    collection = db[collection_name]
                    doc_count = collection.count_documents({})
                    logger.info(f"HEALTH CENTER: Collection {collection_name} has {doc_count} documents")
                    
                    if doc_count > 0:
                        # Get sample document to understand structure
                        sample_doc = collection.find_one()
                        if sample_doc:
                            logger.info(f"HEALTH CENTER: Sample from {collection_name} - keys: {list(sample_doc.keys())}")
                        
                        # Try multiple field name variations for each filter type
                        year_fields = ['_year', 'year', 'Year', 'test_year', 'date_year']
                        district_fields = ['_district', 'district', 'District']
                        sector_fields = ['_sector', 'sector', 'Sector']
                        
                        # Get years
                        for year_field in year_fields:
                            try:
                                years_from_data = collection.distinct(year_field)
                                for y in years_from_data:
                                    if y and isinstance(y, (int, str)):
                                        try:
                                            year_int = int(y)
                                            if 2015 <= year_int <= 2030:
                                                all_years.add(year_int)
                                        except (ValueError, TypeError):
                                            continue
                                if years_from_data:  # If we found years in this field, break
                                    logger.info(f"HEALTH CENTER: Found years in field '{year_field}': {years_from_data}")
                                    break
                            except Exception:
                                continue
                        
                        # Get districts
                        for district_field in district_fields:
                            try:
                                districts_from_data = collection.distinct(district_field)
                                for d in districts_from_data:
                                    if d and str(d).strip():
                                        all_districts.add(str(d).strip())
                                if districts_from_data:
                                    logger.info(f"HEALTH CENTER: Found districts in field '{district_field}': {districts_from_data}")
                                    break
                            except Exception:
                                continue
                        
                        # Get sectors
                        for sector_field in sector_fields:
                            try:
                                sectors_from_data = collection.distinct(sector_field)
                                for s in sectors_from_data:
                                    if s and str(s).strip():
                                        all_sectors.add(str(s).strip())
                                if sectors_from_data:
                                    logger.info(f"HEALTH CENTER: Found sectors in field '{sector_field}': {sectors_from_data}")
                                    break
                            except Exception:
                                continue
                                
                except Exception as col_error:
                    logger.error(f"HEALTH CENTER: Error processing collection {collection_name}: {str(col_error)}")
                    continue
            
            result = {
                'years': sorted(list(all_years)),
                'districts': sorted(list(all_districts)),
                'sectors': sorted(list(all_sectors))
            }
            
            logger.info(f"HEALTH CENTER FILTERS: years={result['years']}, districts={result['districts']}, sectors={result['sectors']}")
            return result
            
        except Exception as e:
            logger.error(f"HEALTH CENTER: Error getting filters: {str(e)}")
            logger.error(f"HEALTH CENTER: Traceback: {traceback.format_exc()}")
            return {'years': [], 'districts': [], 'sectors': []}
    
    def extract_data_for_analytics(self, district: Optional[str] = None, 
                                 sector: Optional[str] = None, 
                                 years: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """Extract health center data using document filtering"""
        try:
            start_time = time.time()
            client = self._connect()
            db = client[self.mongo_db]
            
            # Find the actual collection that exists
            all_collections = db.list_collection_names()
            hc_collections = [col for col in all_collections if 'healthcenter-data' in col and not col.endswith('_metadata')]
            
            if not hc_collections:
                logger.warning("HEALTH CENTER: No collections found")
                return []
            
            all_documents = []
            
            # Process each health center collection
            for collection_name in hc_collections:
                try:
                    collection = db[collection_name]
                    
                    # Build query filter for documents within the collection
                    query = {}
                    
                    # Add district filter (try multiple field names)
                    if district:
                        district_conditions = [
                            {'_district': {'$regex': f'^{district}$', '$options': 'i'}},
                            {'district': {'$regex': f'^{district}$', '$options': 'i'}},
                            {'District': {'$regex': f'^{district}$', '$options': 'i'}}
                        ]
                        query['$or'] = district_conditions
                    
                    # Add sector filter
                    if sector:
                        sector_conditions = [
                            {'_sector': {'$regex': f'^{sector}$', '$options': 'i'}},
                            {'sector': {'$regex': f'^{sector}$', '$options': 'i'}},
                            {'Sector': {'$regex': f'^{sector}$', '$options': 'i'}}
                        ]
                        if '$or' in query:
                            query = {'$and': [query, {'$or': sector_conditions}]}
                        else:
                            query['$or'] = sector_conditions
                    
                    # Add year filter (try multiple field names)
                    if years:
                        year_conditions = [
                            {'_year': {'$in': years}},
                            {'year': {'$in': years}},
                            {'Year': {'$in': years}}
                        ]
                        if '$and' in query:
                            query['$and'].append({'$or': year_conditions})
                        elif '$or' in query:
                            query = {'$and': [query, {'$or': year_conditions}]}
                        else:
                            query['$or'] = year_conditions
                    
                    logger.info(f"HEALTH CENTER: Querying {collection_name} with: {query}")
                    
                    # Execute query
                    cursor = collection.find(query).batch_size(1000)
                    documents = list(cursor)
                    
                    logger.info(f"HEALTH CENTER: Collection {collection_name} returned {len(documents)} documents")
                    
                    # Add collection source info to each document and clean up
                    for doc in documents:
                        doc['_source_collection'] = collection_name
                        # Remove MongoDB ObjectId if present
                        if '_id' in doc:
                            del doc['_id']
                    
                    all_documents.extend(documents)
                    
                except Exception as collection_error:
                    logger.error(f"HEALTH CENTER: Error processing {collection_name}: {str(collection_error)}")
                    continue
            
            total_time = time.time() - start_time
            logger.info(f"HEALTH CENTER EXTRACTION: Found {len(all_documents)} total documents in {total_time:.2f} seconds")
            
            if all_documents:
                sample_doc = all_documents[0]
                logger.info(f"HEALTH CENTER SAMPLE STRUCTURE: {list(sample_doc.keys())}")
                
                # Log some sample field values to help with debugging
                sample_values = {}
                for key in ['_year', 'year', 'Year', '_district', 'district', '_sector', 'sector']:
                    if key in sample_doc:
                        sample_values[key] = sample_doc[key]
                logger.info(f"HEALTH CENTER SAMPLE VALUES: {sample_values}")
            
            return all_documents
            
        except Exception as e:
            logger.error(f"HEALTH CENTER: Data extraction failed: {str(e)}")
            logger.error(f"HEALTH CENTER: Traceback: {traceback.format_exc()}")
            return []
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            self.client = None
            logger.info("HEALTH CENTER: MongoDB connection closed")