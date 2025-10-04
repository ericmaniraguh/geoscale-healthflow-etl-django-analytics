# =============================================================================
# 5. FILE: processors/progress_tracker.py
# =============================================================================

import uuid
import threading
from datetime import datetime
from typing import Dict, Any, Optional
from pymongo import MongoClient
import os

class ProgressTracker:
    """Thread-safe progress tracking using MongoDB"""
    
    _lock = threading.Lock()
    _client = None
    _db = None
    _collection = None
    
    @classmethod
    def _get_collection(cls):
        """Get MongoDB collection for progress tracking"""
        if cls._collection is None:
            try:
                cls._client = MongoClient(os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'))
                cls._db = cls._client[os.getenv('MONGODB_DB', 'geospatial_db')]
                cls._collection = cls._db['processing_status']
            except Exception as e:
                print(f"MongoDB connection error: {e}")
                return None
        return cls._collection
    
    @classmethod
    def create_process(cls) -> str:
        """Create new process and return process ID"""
        process_id = str(uuid.uuid4())
        
        initial_status = {
            "process_id": process_id,
            "stage": "upload",
            "progress": 0,
            "total": 100,
            "message": "Initializing...",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "preview_data": [],
            "error": None,
            "completed": False
        }
        
        collection = cls._get_collection()
        if collection:
            try:
                collection.insert_one(initial_status)
            except Exception as e:
                print(f"Error creating process: {e}")
        
        return process_id
    
    @classmethod
    def update(cls, process_id: str, stage: int, progress: int, message: str, 
               preview_data: Optional[list] = None, error: Optional[str] = None):
        """Update progress status"""
        with cls._lock:
            collection = cls._get_collection()
            if not collection:
                return
            
            stage_names = {
                0: "upload",
                1: "validation", 
                2: "processing",
                3: "merging",
                4: "completed"
            }
            
            update_data = {
                "stage": stage_names.get(stage, "unknown"),
                "progress": progress,
                "message": message,
                "updated_at": datetime.now().isoformat()
            }
            
            if preview_data:
                update_data["preview_data"] = preview_data
            
            if error:
                update_data["error"] = error
                update_data["completed"] = True
            
            if progress >= 100 and stage >= 4:
                update_data["completed"] = True
            
            try:
                collection.update_one(
                    {"process_id": process_id},
                    {"$set": update_data}
                )
            except Exception as e:
                print(f"Error updating progress: {e}")
    
    @classmethod
    def get_status(cls, process_id: str) -> Dict[str, Any]:
        """Get current status"""
        collection = cls._get_collection()
        if not collection:
            return {
                "stage": "error",
                "progress": 0,
                "message": "Database connection error",
                "error": "MongoDB not available"
            }
        
        try:
            status = collection.find_one({"process_id": process_id})
            if status:
                # Remove MongoDB _id field
                status.pop('_id', None)
                return status
            else:
                return {
                    "stage": "unknown",
                    "progress": 0,
                    "message": "Process not found",
                    "error": "Invalid process ID"
                }
        except Exception as e:
            return {
                "stage": "error", 
                "progress": 0,
                "message": f"Status check failed: {str(e)}",
                "error": str(e)
            }
    
    @classmethod
    def cleanup_old_processes(cls, hours: int = 24):
        """Clean up old process records"""
        collection = cls._get_collection()
        if not collection:
            return
        
        try:
            from datetime import datetime, timedelta
            cutoff = datetime.now() - timedelta(hours=hours)
            collection.delete_many({
                "created_at": {"$lt": cutoff.isoformat()}
            })
        except Exception as e:
            print(f"Cleanup error: {e}")
