
import pytest
from unittest.mock import MagicMock, patch, ANY
from django.urls import reverse
from django.contrib.auth import get_user_model
from django.test import Client, RequestFactory
from django.core.files.uploadedfile import SimpleUploadedFile
import json
import os
import tempfile
from app.geospatial_merger import views

@pytest.fixture
def admin_user(db):
    User = get_user_model()
    user = User.objects.create_superuser(username='admin', password='password', email='admin@example.com')
    user.refresh_from_db()
    return user

@pytest.fixture
def normal_user(db):
    User = get_user_model()
    user = User.objects.create_user(username='user', password='password')
    return user

@pytest.fixture
def client():
    return Client()

@pytest.mark.django_db
class TestGeospatialMergerViews:
    
    def test_dashboard_access_denied_for_normal_user(self, client, normal_user):
        client.force_login(normal_user)
        response = client.get(reverse('geospatial_merger:dashboard'))
        assert response.status_code == 302 # Redirect to access denied

    def test_dashboard_access_granted_for_admin(self, client, admin_user):
        client.force_login(admin_user)
        response = client.get(reverse('geospatial_merger:dashboard'))
        assert response.status_code == 200
        # The template uses "Complete Geospatial Processing Dashboard" in the title tag
        assert 'Complete Geospatial Processing Dashboard' in response.content.decode()

    @patch('app.geospatial_merger.views.save_file')
    def test_upload_files_success(self, mock_save_file, client, admin_user):
        client.force_login(admin_user)
        
        geojson = SimpleUploadedFile("test.geojson", b"{}", content_type="application/json")
        geotiff = SimpleUploadedFile("test.tif", b"data", content_type="image/tiff")
        
        mock_save_file.side_effect = ["/tmp/test.geojson", "/tmp/test.tif"]
        
        response = client.post(reverse('geospatial_merger:upload_files'), {
            'geojson': geojson,
            'geotiff': geotiff
        })
        
        assert response.status_code == 200
        data = json.loads(response.content)
        assert data['success'] is True
        assert 'process_id' in data

    @patch('app.geospatial_merger.views.threading.Thread')
    def test_start_merge_process(self, mock_thread, client, admin_user):
        client.force_login(admin_user)
        
        # Setup session
        session = client.session
        session['geojson_path_proc123'] = '/tmp/geo.json'
        session['geotiff_path_proc123'] = '/tmp/geo.tif'
        session.save()
        
        response = client.post(reverse('geospatial_merger:api_start_merge'), {
            'process_id': 'proc123'
        })
        
        assert response.status_code == 200
        data = json.loads(response.content)
        assert data['success'] is True
        mock_thread.return_value.start.assert_called_once()

    def test_get_processing_status_file_found(self, client, admin_user):
        client.force_login(admin_user)
        
        with patch('app.geospatial_merger.views.os.path.exists') as mock_exists:
            with patch('builtins.open', new_callable=MagicMock) as mock_open:
                mock_exists.return_value = True
                mock_open.return_value.__enter__.return_value.read.return_value = '{"progress": 50}'
                # json.load reads from file object, so we mock read
                # actually json.load calls read() on the file object
                
                # We need to act as if we are opening a real file for json.load
                # Easiest is to mock json.load or use proper open mock
                with patch('app.geospatial_merger.views.json.load') as mock_json_load:
                    mock_json_load.return_value = {"progress": 50, "stage": "processing"}
                    
                    response = client.get(reverse('geospatial_merger:api_status'), {'process_id': 'proc123'})
                    
                    assert response.status_code == 200
                    data = json.loads(response.content)
                    assert data['progress'] == 50

    @patch('app.geospatial_merger.views.MongoClient')
    def test_get_results_preview_mongo(self, mock_mongo, client, admin_user):
        client.force_login(admin_user)
        
        mock_db = mock_mongo.return_value.__getitem__.return_value
        mock_col = mock_db.__getitem__.return_value
        # Mock cursor
        cursor = MagicMock()
        cursor.limit.return_value = [{"feature_id": 1, "slope": 10}, {"feature_id": 2, "slope": 20}]
        mock_col.find.return_value = cursor
        
        response = client.get(reverse('geospatial_merger:api_preview'), {'process_id': 'proc123'})
        
        assert response.status_code == 200
        data = json.loads(response.content)
        assert len(data['preview']) == 2
        
    @patch('app.geospatial_merger.views.MongoSaver')
    def test_get_global_stats(self, mock_saver_cls, client, admin_user):
        client.force_login(admin_user)
        
        mock_saver = mock_saver_cls.return_value
        mock_saver.get_global_statistics.return_value = {"total_boundaries": 100}
        
        response = client.get(reverse('geospatial_merger:api_global_stats'))
        
        assert response.status_code == 200
        data = json.loads(response.content)
        assert data['total_boundaries'] == 100
