# =============================================================================
# GEOSPATIAL MONGODB SAVER SERVICE
# File: app/geospatial_merger/services/mongo_saver.py
# =============================================================================

import logging
import traceback
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, BulkWriteError
from django.conf import settings
from datetime import datetime

logger = logging.getLogger(__name__)

def log_debug(message):
    """Log debug message to a file"""
    try:
        import tempfile
        import os
        log_file = os.path.join(tempfile.gettempdir(), "geospatial_mongo_debug.log")
        with open(log_file, "a") as f:
            f.write(f"{datetime.now().isoformat()} - {message}\n")
    except:
        pass


class GeospatialMongoSaver:
    """MongoDB saver service for geospatial data following ETL pattern"""
    
    def __init__(self, process_id: str):
        self.process_id = process_id
        
        # Get MongoDB URI from Django settings (same as ETL)
        self.mongo_uri = getattr(settings, 'MONGO_URI', None)
        
        # Use your existing MongoDB cluster for geospatial data
        self.mongo_db_name = getattr(settings, 'MONGO_SHAPEFILE_DB', 'geospatial_wgs84_boundaries_db')
        
        # Collections
        self.main_collection_name = getattr(settings, 'MONGO_SHAPEFILE_COLLECTION', 'boundaries_slope_wgs84')
        self.metadata_collection_name = 'processing_metadata'
        self.logs_collection_name = 'merge_operation_logs'
        
        self._client = None
        self.mongodb_available = False
        self.mongodb_error = None
        
        # Initialize connection
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize MongoDB connection using Django settings"""
        try:
            if not self.mongo_uri:
                raise Exception("MONGO_URI not found in Django settings")
            
            print("=" * 80)
            print("GEOSPATIAL MONGODB CONNECTION SETUP")
            print("=" * 80)
            print(f"Using Django settings MONGO_URI: {self.mongo_uri.split('@')[0] if '@' in self.mongo_uri else 'localhost'}@***")
            log_debug(f"Process {self.process_id}: Initializing MongoDB connection. URI provided: {'Yes' if self.mongo_uri else 'No'}")
            
            # Connect with optimized settings
            self._client = MongoClient(
                self.mongo_uri,
                serverSelectionTimeoutMS=30000,
                connectTimeoutMS=30000,
                socketTimeoutMS=30000,
                maxPoolSize=50,
                retryWrites=True,
                w="majority"
            )
            
            # Test connection
            self._client.admin.command("ping")
            print("MongoDB connection successful")
            log_debug(f"Process {self.process_id}: MongoDB connection successful.")
            
            # Set up database and collections
            self.db = self._client[self.mongo_db_name]
            self.collection = self.db[self.main_collection_name]
            self.metadata_collection = self.db[self.metadata_collection_name]
            self.logs_collection = self.db[self.logs_collection_name]
            
            # Test write access
            test_doc = {
                "test": "geospatial_connection",
                "timestamp": datetime.now().isoformat(),
                "process_id": self.process_id,
                "coordinate_system": "WGS84"
            }
            result = self.metadata_collection.insert_one(test_doc)
            self.metadata_collection.delete_one({"_id": result.inserted_id})
            
            self.mongodb_available = True
            print(f"Database: {self.mongo_db_name}")
            print("Collections configured:")
            print(f"  - {self.main_collection_name} (main data)")
            print(f"  - {self.metadata_collection_name}")
            print(f"  - {self.logs_collection_name}")
            print("=" * 80)
            
        except Exception as e:
            self.mongodb_error = str(e)
            log_debug(f"Process {self.process_id}: MongoDB connection FAILED. Error: {e}")
            print(f"MongoDB connection failed: {e}")
            print("Will use file storage only")
            print("=" * 80)
    
    @property
    def client(self):
        """Get MongoDB client instance"""
        return self._client
    
    @property
    def database(self):
        """Get MongoDB database instance"""
        if self.client:
            return self.client[self.mongo_db_name]
        return None
    
    def is_connected(self) -> bool:
        """Check if MongoDB is connected"""
        return self.mongodb_available
    
    def get_connection_status(self) -> Dict[str, Any]:
        """Get detailed connection status"""
        return {
            "available": self.mongodb_available,
            "error": self.mongodb_error,
            "database": self.mongo_db_name,
            "process_id": self.process_id
        }
    
    def save_batch_results(self, batch_results: List[Dict], batch_number: int) -> bool:
        """Save batch of WGS84 results to MongoDB"""
        if not self.mongodb_available:
            logger.warning(f"MongoDB not available for batch {batch_number}")
            return False
        
        try:
            if not batch_results:
                return True
            
            # Add batch metadata to each document
            for result in batch_results:
                result["_batch_info"] = {
                    "batch_number": batch_number,
                    "process_id": self.process_id,
                    "saved_at": datetime.now().isoformat(),
                    "coordinate_system": "WGS84"
                }
            
            # Insert batch
            insert_result = self.collection.insert_many(batch_results, ordered=False)
            
            # Log successful batch save
            self.log_batch_operation(batch_number, len(batch_results), "SUCCESS")
            
            print(f"   Saved batch {batch_number}: {len(insert_result.inserted_ids)} documents")
            return True
            
        except BulkWriteError as e:
            error_msg = f"Bulk write error in batch {batch_number}: {e.details}"
            logger.error(error_msg)
            self.log_batch_operation(batch_number, len(batch_results), "PARTIAL_FAILURE", error_msg)
            return False
            
        except Exception as e:
            error_msg = f"Error saving batch {batch_number}: {str(e)}"
            logger.error(error_msg)
            self.log_batch_operation(batch_number, len(batch_results), "FAILURE", error_msg)
            return False
    
    def save_all_results(self, results: List[Dict]) -> bool:
        """Save all results at once (fallback method)"""
        if not self.mongodb_available:
            logger.warning("MongoDB not available for saving all results")
            return False
        
        try:
            # Check if data already exists for this process
            existing_count = self.collection.count_documents({
                "processing_metadata.process_id": self.process_id
            })
            
            if existing_count > 0:
                print(f"Found {existing_count} existing records for process {self.process_id}")
                return True
            
            if not results:
                print("No results to save")
                return True
            
            # Add metadata to all documents
            for result in results:
                result["_save_info"] = {
                    "process_id": self.process_id,
                    "saved_at": datetime.now().isoformat(),
                    "coordinate_system": "WGS84",
                    "save_method": "bulk"
                }
            
            # Insert all results
            insert_result = self.collection.insert_many(results, ordered=False)
            
            # Log successful save
            self.log_save_operation(len(results), "SUCCESS")
            
            print(f"Saved {len(insert_result.inserted_ids)} total records to MongoDB")
            return True
            
        except Exception as e:
            error_msg = f"Error saving all results: {str(e)}"
            logger.error(error_msg)
            self.log_save_operation(len(results), "FAILURE", error_msg)
            return False
    
    def update_progress_metadata(self, stage: str, percentage: int, message: str, 
                               file_stats: Dict = None, coordinate_info: Dict = None):
        """Update progress metadata in MongoDB"""
        if not self.mongodb_available:
            return
        
        progress_data = {
            "process_id": self.process_id,
            "stage": stage,
            "progress": percentage,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "coordinate_system": "WGS84",
            "mongodb_status": self.get_connection_status(),
            "completed": stage == "completed"
        }
        
        if file_stats:
            progress_data["file_statistics"] = file_stats
            
        if coordinate_info:
            progress_data["coordinate_info"] = coordinate_info
        
        try:
            self.metadata_collection.update_one(
                {"process_id": self.process_id},
                {"$set": progress_data},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error updating progress metadata: {e}")
    
    def log_batch_operation(self, batch_number: int, record_count: int, 
                          status: str, error_message: str = None):
        """Log batch operation details"""
        if not self.mongodb_available:
            return
        
        log_entry = {
            "process_id": self.process_id,
            "operation_type": "batch_save",
            "batch_number": batch_number,
            "record_count": record_count,
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "coordinate_system": "WGS84"
        }
        
        if error_message:
            log_entry["error_message"] = error_message
        
        try:
            self.logs_collection.insert_one(log_entry)
        except Exception as e:
            logger.error(f"Error logging batch operation: {e}")
    
    def log_save_operation(self, total_records: int, status: str, error_message: str = None):
        """Log overall save operation"""
        if not self.mongodb_available:
            return
        
        log_entry = {
            "process_id": self.process_id,
            "operation_type": "bulk_save",
            "total_records": total_records,
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "coordinate_system": "WGS84"
        }
        
        if error_message:
            log_entry["error_message"] = error_message
        
        try:
            self.logs_collection.insert_one(log_entry)
        except Exception as e:
            logger.error(f"Error logging save operation: {e}")
    
    def get_process_statistics(self) -> Dict[str, Any]:
        """Get statistics for the current process"""
        if not self.mongodb_available:
            return {}
        
        try:
            # Count total records
            total_records = self.collection.count_documents({
                "processing_metadata.process_id": self.process_id
            })
            
            # Get success/failure counts
            successful_records = self.collection.count_documents({
                "processing_metadata.process_id": self.process_id,
                "processing_metadata.status": {"$ne": "error"}
            })
            
            failed_records = total_records - successful_records
            
            # Get processing metadata
            metadata = self.metadata_collection.find_one({"process_id": self.process_id})
            
            return {
                "total_records": total_records,
                "successful_records": successful_records,
                "failed_records": failed_records,
                "success_rate": (successful_records / total_records * 100) if total_records > 0 else 0,
                "process_metadata": metadata
            }
            
        except Exception as e:
            logger.error(f"Error getting process statistics: {e}")
            return {}

    def get_global_statistics(self) -> Dict[str, Any]:
        """Get global statistics for the dashboard"""
        if not self.mongodb_available:
            return {}

        try:
            # Total boundaries (documents in main collection)
            total_boundaries = self.collection.count_documents({})
            
            # Aggregate processing logs to get total files/slope points analyzed
            pipeline = [
                {
                    "$group": {
                        "_id": None,
                        "total_slope_points": {"$sum": "$file_statistics.total_slope_points"},
                        "total_files": {"$sum": 1} # Count of metadata docs = count of processes
                    }
                }
            ]
            
            meta_stats = list(self.metadata_collection.aggregate(pipeline))
            total_slope_points = meta_stats[0]['total_slope_points'] if meta_stats else 0
            
            return {
                "total_boundaries": total_boundaries,
                "total_slope_points": total_slope_points,
                "total_processes": self.metadata_collection.count_documents({}),
                "last_update": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting global statistics: {e}")
            return {}

    def get_global_statistics(self) -> Dict[str, Any]:
        """Get global statistics for the dashboard"""
        if not self.mongodb_available:
            return {}

        try:
            # Total boundaries (documents in main collection)
            total_boundaries = self.collection.count_documents({})
            
            # Sum of slope points (if available in documents)
            # This can be expensive, so maybe we rely on metadata aggregation
            # Or just count documents for now as "Processed Boundaries"
            
            # Aggregate processing logs to get total files/slope points analyzed
            pipeline = [
                {
                    "$group": {
                        "_id": None,
                        "total_slope_points": {"$sum": "$file_statistics.total_slope_points"},
                        "total_files": {"$sum": 1} # Count of metadata docs = count of processes
                    }
                }
            ]
            
            meta_stats = list(self.metadata_collection.aggregate(pipeline))
            total_slope_points = meta_stats[0]['total_slope_points'] if meta_stats else 0
            
            return {
                "total_boundaries": total_boundaries,
                "total_slope_points": total_slope_points,
                "total_processes": self.metadata_collection.count_documents({}),
                "last_update": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting global statistics: {e}")
            return {}
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            self._client = None
            self.mongodb_available = False
    
    def __del__(self):
        """Cleanup on deletion"""
        self.close_connection()


# Utility function to create saver instance
def create_geospatial_saver(process_id: str) -> GeospatialMongoSaver:
    """Create a new GeospatialMongoSaver instance"""
    return GeospatialMongoSaver(process_id)