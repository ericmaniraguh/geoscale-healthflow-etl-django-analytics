
import os
import sys
import django
import json

# Setup Django environment
sys.path.append(os.getcwd())
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'app.settings')
django.setup()

from django.test import RequestFactory
from app.analytics_dashboard.views import get_map_data
from django.contrib.auth import get_user_model
User = get_user_model()

def test_map_data():
    factory = RequestFactory()
    
    # Create a dummy user for login_required
    try:
        user = User.objects.get(username='admin')
    except User.DoesNotExist:
        user = User.objects.create_superuser('admin', 'admin@example.com', 'admin')

    print("--- Testing MongoDB Source (Existing) ---")
    request_mongo = factory.get('/analytics_dashboard/api/map-data/?province=Iburasirazuba&district=Bugesera&source=mongo')
    request_mongo.user = user
    response_mongo = get_map_data(request_mongo)
    
    if response_mongo.status_code == 200:
        data = json.loads(response_mongo.content)
        print(f"Mongo features found: {len(data.get('features', []))}")
        if data.get('features'):
            print(f"Sample Mongo Feature properties: {data['features'][0]['properties']}")
    else:
        print(f"Mongo Error: {response_mongo.status_code}")

    print("\n--- Testing PostgreSQL Source (New) ---")
    # Using Bugesera/Kamabuye as known test case from user prompt
    request_pg = factory.get('/analytics_dashboard/api/map-data/?province=Iburasirazuba&district=Bugesera&sector=Kamabuye&source=postgres')
    request_pg.user = user
    response_pg = get_map_data(request_pg)
    
    if response_pg.status_code == 200:
        data = json.loads(response_pg.content)
        print(f"Postgres features found: {len(data.get('features', []))}")
        if data.get('features'):
            print(f"Sample Postgres Feature properties: {data['features'][0]['properties']}")
    else:
        print(f"Postgres Error: {response_pg.status_code}")

if __name__ == "__main__":
    test_map_data()
