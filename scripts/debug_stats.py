
import os
import sys
import django
from django.conf import settings

# Setup Django environment
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")
django.setup()

from app.geospatial_merger.processors.mongo_saver import GeospatialMongoSaver

def debug_global_stats():
    print("Initializing GeospatialMongoSaver...")
    try:
        saver = GeospatialMongoSaver("debug_stats_script")
        print(f"MongoDB Available: {saver.mongodb_available}")
        
        if saver.mongodb_error:
            print(f"MongoDB Error: {saver.mongodb_error}")
            return

        print("Calling get_global_statistics()...")
        stats = saver.get_global_statistics()
        print(f"Returned Stats: {stats}")
        
        if not stats:
            print("Stats are empty! Checking collection counts directly...")
            print(f"Main Collection Count: {saver.collection.count_documents({})}")
            print(f"Metadata Collection Count: {saver.metadata_collection.count_documents({})}")

    except Exception as e:
        print(f"EXCEPTION: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_global_stats()
