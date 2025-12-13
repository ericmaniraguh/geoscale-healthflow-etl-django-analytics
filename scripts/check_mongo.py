
import os
import sys
from django.conf import settings
from pymongo import MongoClient


def run():
    print("="*50)
    print("DEBUGGING MONGODB CONNECTION")
    print("="*50)
    
    uri = getattr(settings, 'MONGO_URI', None)
    db_name = getattr(settings, 'MONGO_SHAPEFILE_DB', 'geospatial_wgs84_boundaries_db')
    
    print(f"MONGO_URI: {uri}")
    print(f"Target DB: {db_name}")
    
    if not uri:
        print("ERROR: MONGO_URI is not set in settings!")
        return

    try:
        print("Attempting to connect...")
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("SUCCESS: Connected to MongoDB!")
        

        db = client[db_name]
        cols = db.list_collection_names()
        print(f"Existing collections in {db_name}: {cols}")
        

        main_col = getattr(settings, 'MONGO_SHAPEFILE_COLLECTION', 'boundaries_slope_wgs84')
        meta_col = 'processing_metadata'
        logs_col = 'merge_operation_logs'
        
        for col_name in [main_col, meta_col, logs_col]:
            if col_name in cols:
                count = db[col_name].count_documents({})
                print(f"Documents in '{col_name}': {count}")
                if count > 0:
                    print(f"--- First doc in {col_name} ---")
                    print(db[col_name].find_one())
                    print("-----------------------------")
            else:
                print(f"Collection '{col_name}' NOT found.")

            
    except Exception as e:

        print(f"FAILURE: Could not connect to MongoDB.")
        print(f"Error: {str(e)}")

