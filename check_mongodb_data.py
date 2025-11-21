"""
MongoDB Data Verification Script
Checks if weather and malaria data exists in MongoDB
"""

import os
import sys
from pymongo import MongoClient
from decouple import config

# MongoDB Configuration
MONGO_URI = config('MONGO_URI', default='mongodb://localhost:27017/')
MONGO_WEATHER_DB = config('MONGO_WEATHER_DB', default='weather_data')
MONGO_HMIS_DB = config('MONGO_HMIS_DB', default='hmis_data')

def check_mongodb_collections():
    """Check what data exists in MongoDB"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

        # Test connection
        client.admin.command('ismaster')
        print("[SUCCESS] MongoDB connection successful!\n")

        # Check Weather Database
        print("=" * 70)
        print("WEATHER DATABASE")
        print("=" * 70)
        weather_db = client[MONGO_WEATHER_DB]
        weather_collections = weather_db.list_collection_names()

        if not weather_collections:
            print("[ERROR] No collections found in weather database")
        else:
            print(f"[SUCCESS] Found {len(weather_collections)} collection(s):\n")

            for coll_name in weather_collections:
                collection = weather_db[coll_name]
                count = collection.count_documents({})
                print(f"  [Collection] {coll_name}")
                print(f"     Records: {count}")

                if count > 0:
                    sample = collection.find_one({})
                    if sample:
                        # Check for metadata
                        if '_station' in sample:
                            print(f"     Station: {sample.get('_station', 'N/A')}")
                            print(f"     Data Type: {sample.get('_data_type', 'N/A')}")
                            print(f"     Years: {sample.get('_dataset_years', 'N/A')}")
                        # Check if it's temperature or precipitation data
                        if 'TMPMAX' in sample:
                            print(f"     Type: Temperature Data")
                        elif 'PRECIP' in sample:
                            print(f"     Type: Precipitation Data")
                        print(f"     Sample fields: {list(sample.keys())[:10]}")
                print()

        # Check Malaria/HMIS Database
        print("=" * 70)
        print("MALARIA/HMIS DATABASE")
        print("=" * 70)
        hmis_db = client[MONGO_HMIS_DB]
        hmis_collections = hmis_db.list_collection_names()

        if not hmis_collections:
            print("[ERROR] No collections found in malaria/HMIS database")
            print("   [WARNING] This explains the 'No malaria datasets found' error!")
        else:
            print(f"[SUCCESS] Found {len(hmis_collections)} collection(s):\n")

            for coll_name in hmis_collections:
                collection = hmis_db[coll_name]
                count = collection.count_documents({})
                print(f"  [Collection] {coll_name}")
                print(f"     Records: {count}")

                if count > 0:
                    sample = collection.find_one({})
                    if sample:
                        # Check for malaria-specific fields
                        if 'Province' in sample and 'District' in sample:
                            print(f"     Province: {sample.get('Province', 'N/A')}")
                            print(f"     District: {sample.get('District', 'N/A')}")
                            print(f"     Sector: {sample.get('Sector', 'N/A')}")
                        # Check for year columns
                        year_cols = [k for k in sample.keys() if 'Total Cases_' in k or 'Pop' in k]
                        if year_cols:
                            print(f"     Year columns found: {year_cols[:3]}...")
                        print(f"     Sample fields: {list(sample.keys())[:10]}")
                print()

        print("=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"Weather Collections: {len(weather_collections)}")
        print(f"Malaria Collections: {len(hmis_collections)}")

        if not hmis_collections:
            print("\n[WARNING] ACTION REQUIRED:")
            print("   You need to upload malaria data to MongoDB first!")
            print("   Visit: http://localhost:8000/upload/admin-dashboard/")
            print("   And upload HMIS malaria data files")

        if weather_collections and not hmis_collections:
            print("\n[SUCCESS] Weather data exists - you can run weather ETL")
            print("[ERROR] Malaria data missing - cannot run malaria ETL")
        elif weather_collections and hmis_collections:
            print("\n[SUCCESS] Both weather and malaria data exist!")
            print("   You can run both ETL processes")

        client.close()

    except Exception as e:
        print(f"[ERROR] Error: {str(e)}")
        print("\nTroubleshooting:")
        print("1. Check if MongoDB is running")
        print("2. Check MONGO_URI in your .env file")
        print("3. Verify database names in .env file")

if __name__ == "__main__":
    check_mongodb_collections()
