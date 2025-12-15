
import pytest
from unittest.mock import MagicMock, patch, ANY
import json
from django.urls import reverse
from django.contrib.auth import get_user_model
from django.test import Client, RequestFactory
from django.contrib.messages import get_messages
from django.core.files.uploadedfile import SimpleUploadedFile
from app.upload_app.views import dashboard_view

@pytest.fixture
def admin_user(db):
    User = get_user_model()
    # Explicitly set is_staff=True and is_superuser=True
    user = User.objects.create_superuser(username='admin', password='password', email='admin@example.com')
    user.is_staff = True
    user.is_superuser = True
    user.save()
    user.refresh_from_db()
    return user

@pytest.fixture
def clean_user(db):
    User = get_user_model()
    user = User.objects.create_user(username='user', password='password', email='user@example.com')
    user.refresh_from_db()
    return user

@pytest.fixture
def client_admin(admin_user):
    client = Client()
    client.force_login(admin_user)
    return client

@pytest.fixture
def client_user(clean_user):
    client = Client()
    client.force_login(clean_user)
    return client

@pytest.mark.django_db
class TestDashboardViews:
    
    def test_upload_dashboard_redirects(self, client_admin, client_user):
        # Admin -> Admin Dashboard
        response = client_admin.get(reverse('upload_app:upload_dashboard'))
        assert response.status_code == 302
        assert response.url == reverse('upload_app:admin_dashboard')
        
        # User -> User Dashboard
        response = client_user.get(reverse('upload_app:upload_dashboard'))
        assert response.status_code == 302
        assert response.url == reverse('upload_app:user_dashboard')

    def test_user_dashboard_access(self, client_user):
        response = client_user.get(reverse('upload_app:user_dashboard'))
        assert response.status_code == 200
        # Check template name inclusion correctly
        assert any('upload_app/user_dashboard.html' in t.name for t in response.templates)

    def test_admin_dashboard_access_denied_for_user(self, client_user):
        response = client_user.get(reverse('upload_app:admin_dashboard'))
        assert response.status_code == 302
        assert response.url == reverse('upload_app:user_dashboard')

    def test_admin_dashboard_access_granted(self, client_admin):
        response = client_admin.get(reverse('upload_app:admin_dashboard'))
        assert response.status_code == 200
        assert any('upload_app/admin_dashboard.html' in t.name for t in response.templates)

    # Test Bridge Handlers
    
    @patch('app.upload_app.views.dashboard_view.GeoTiffUploadView')
    def test_upload_slope_geojson_bridge(self, mock_view_cls, client_admin):
        # Setup mock
        mock_view_instance = mock_view_cls.return_value
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.data = {'features_count': 100}
        mock_view_instance.post.return_value = mock_response
        
        # File
        tif_file = SimpleUploadedFile("test.tif", b"data", content_type="image/tiff")
        
        response = client_admin.post(
            reverse('upload_app:slope-geojson'),
            {'tif_file': tif_file, 'region': 'Kigali'},
            follow=True
        )
        
        assert response.status_code == 200 # Redirects back to dashboard
        messages = list(get_messages(response.wsgi_request))
        assert any("Successfully converted" in str(m) for m in messages)
        mock_view_instance.post.assert_called_once()

    @patch('app.upload_app.views.dashboard_view.UploadShapefileCountryView')
    def test_upload_country_shapefile_bridge(self, mock_view_cls, client_admin):
        mock_view_instance = mock_view_cls.return_value
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.data = {'village_analysis': {'total_villages': 50}}
        mock_view_instance.post.return_value = mock_response
        
        file = SimpleUploadedFile("test.zip", b"data", content_type="application/zip")
        
        # Check URL
        url = reverse('upload_app:upload_country_shapefile')

        response = client_admin.post(url, {'file': file, 'dataset_name': 'Test'}, follow=True)
        assert response.status_code == 200
        messages = list(get_messages(response.wsgi_request))
        assert any("Successfully processed" in str(m) for m in messages)
        mock_view_instance.post.assert_called()

    @patch('app.upload_app.views.dashboard_view.UploadHMISAPIDataView')
    def test_upload_hmis_bridge(self, mock_view_cls, client_user):
         mock_view_instance = mock_view_cls.return_value
         mock_response = MagicMock()
         mock_response.status_code = 201
         mock_response.data = {'records_inserted': 5}
         mock_view_instance.post.return_value = mock_response
         
         file = SimpleUploadedFile("hmis.json", b"{}", content_type="application/json")
         
         url = reverse('upload_app:upload_HMIS_malaria_records')

         response = client_user.post(url, {'file': file}, follow=True)
         assert response.status_code == 200
         messages = list(get_messages(response.wsgi_request))
         assert any("Successfully uploaded" in str(m) for m in messages)
         
    @patch('app.upload_app.views.dashboard_view.UploadHealthCenterLabDataView')
    def test_upload_hc_bridge(self, mock_view_cls, client_user):
         mock_view_instance = mock_view_cls.return_value
         mock_response = MagicMock()
         mock_response.status_code = 201
         mock_response.data = {'records_inserted': 10, 'collection_info': {'data_collection': 'coll'}}
         mock_view_instance.post.return_value = mock_response
         
         file = SimpleUploadedFile("hc.csv", b"csv", content_type="text/csv")
         
         url = reverse('upload_app:upload_healthcenter_malaria_records')

         response = client_user.post(url, {'file': file}, follow=True)
         assert response.status_code == 200
         mock_view_instance.post.assert_called()

    # Test Utilities
    
    @patch('app.upload_app.views.dashboard_view.MongoClient')
    def test_test_mongodb_connection(self, mock_client, client_user):
        mock_client.return_value.list_database_names.return_value = ['db1']
        response = client_user.get(reverse('upload_app:test_mongodb_connection'))
        assert response.status_code == 200
        assert json.loads(response.content)['status'] == 'success'
