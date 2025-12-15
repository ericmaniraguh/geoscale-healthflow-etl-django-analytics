
import pytest
import json
from unittest.mock import MagicMock, patch, ANY
from django.test import RequestFactory
from django.contrib.auth.models import AnonymousUser
from django.contrib.messages.storage.fallback import FallbackStorage
from app.etl_app.views.etl_dashboard_view import (
    etl_dashboard, 
    detect_form_type
)

@pytest.fixture
def request_factory():
    return RequestFactory()

@pytest.fixture
def mock_user():
    user = MagicMock()
    user.is_authenticated = True
    user.is_superuser = False
    user.is_staff = False
    user.username = 'testuser'
    return user

@pytest.fixture
def mock_admin_user():
    user = MagicMock()
    user.is_authenticated = True
    user.is_superuser = True
    user.is_staff = True
    user.username = 'admin'
    return user

def setup_request(request, user):
    request.user = user
    # Setup messages storage
    setattr(request, 'session', 'session')
    messages = FallbackStorage(request)
    setattr(request, '_messages', messages)
    return request

class TestEtlDashboardView:
    def test_get_dashboard_authenticated(self, request_factory, mock_user):
        request = request_factory.get('/etl/dashboard/')
        request = setup_request(request, mock_user)
        
        response = etl_dashboard(request)
        assert response.status_code == 200
        # Since we are calling the view function directly, response is HttpResponse
        # with rendered content.
        content = response.content.decode('utf-8')
        assert 'ETL Dashboard' in content
        assert 'Data Processor' in content
        
    def test_get_dashboard_unauthenticated(self, request_factory):
        request = request_factory.get('/etl/dashboard/')
        request.user = AnonymousUser()
        
        # When calling decorated view directly, it might redirect or fail depending on middleware
        # @login_required usually redirects to LOGIN_URL
        response = etl_dashboard(request)
        assert response.status_code == 302
        assert '/accounts/login/' in response.url

    def test_admin_role_context(self, request_factory, mock_admin_user):
        request = request_factory.get('/etl/dashboard/')
        request = setup_request(request, mock_admin_user)
        
        response = etl_dashboard(request)
        assert response.status_code == 200
        content = response.content.decode('utf-8')
        assert 'Admin' in content

class TestDetectFormType:
    def test_detect_explicit_submit(self):
        req = MagicMock()
        req.POST = {'malaria_submit': 'true'}
        assert detect_form_type(req) == 'malaria'

        req.POST = {'health_center_submit': 'true'}
        assert detect_form_type(req) == 'health_center'

    def test_detect_implicit_fields(self):
        req = MagicMock()
        req.POST = {'province': 'Kigali', 'district': 'Gasabo', 'years': '2023'}
        assert detect_form_type(req) == 'malaria'

        req.POST = {'district': 'Gasabo', 'sector': 'Kinyinya', 'years': '2023'}
        assert detect_form_type(req) == 'health_center'
        
        req.POST = {'station_temp': 'Kigali'}
        assert detect_form_type(req) == 'weather'

class TestFormSubmissions:
    @patch('app.etl_app.views.etl_dashboard_view.requests.post')
    def test_malaria_success_ajax(self, mock_post, request_factory, mock_admin_user):
        # Setup mock
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'message': 'Done', 'records_processed': 100}
        mock_post.return_value = mock_response

        # Setup request
        data = {
            'malaria_submit': 'true',
            'province': 'Kigali',
            'district': 'Gasabo',
            'years': '2023'
        }
        request = request_factory.post('/etl/dashboard/', data)
        request = setup_request(request, mock_admin_user)
        request.headers = {'X-Requested-With': 'XMLHttpRequest'} # AJAX
        
        response = etl_dashboard(request)
        
        assert response.status_code == 200
        content = json.loads(response.content)
        assert content['success'] is True
        assert content['records_processed'] == 100
        mock_post.assert_called_once()
    
    @patch('app.etl_app.views.etl_dashboard_view.requests.post')
    def test_health_center_success(self, mock_post, request_factory, mock_admin_user):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'message': 'Success', 'records_processed': 50}
        mock_post.return_value = mock_response

        data = {
            'health_center_submit': 'true',
            'years': '2022',
            'district': 'Gasabo',
            'sector': 'Kinyinya'
        }
        request = request_factory.post('/etl/dashboard/', data)
        request = setup_request(request, mock_admin_user)
        request.headers = {'X-Requested-With': 'XMLHttpRequest'}
        
        response = etl_dashboard(request)
        
        assert response.status_code == 200
        content = json.loads(response.content)
        assert content['success'] is True
        assert 'health_center_data' in content.get('table_name', 'health_center_data')

    @patch('app.etl_app.views.etl_dashboard_view.requests.post')
    def test_weather_success(self, mock_post, request_factory, mock_admin_user):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'message': 'Success'}
        mock_post.return_value = mock_response

        data = {
            'weather_prec_temp_submit': 'true',
            'years': '2023',
            'station_temp': 'Kiegali'
        }
        request = request_factory.post('/etl/dashboard/', data)
        request = setup_request(request, mock_admin_user)
        request.headers = {'X-Requested-With': 'XMLHttpRequest'}
        
        response = etl_dashboard(request)
        assert response.status_code == 200
        assert json.loads(response.content)['success'] is True

    @patch('app.etl_app.views.etl_dashboard_view.requests.post')
    def test_boundaries_success(self, mock_post, request_factory, mock_admin_user):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'message': 'Success'}
        mock_post.return_value = mock_response

        data = {
            'boundaries_submit': 'true',
            'province': 'Kigali',
            'update_mode': 'replace'
        }
        request = request_factory.post('/etl/dashboard/', data)
        request = setup_request(request, mock_admin_user)
        request.headers = {'X-Requested-With': 'XMLHttpRequest'}
        
        response = etl_dashboard(request)
        assert response.status_code == 200
        assert json.loads(response.content)['success'] is True

    @patch('app.etl_app.views.etl_dashboard_view.requests.post')
    def test_slope_success(self, mock_post, request_factory, mock_admin_user):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'message': 'Success'}
        mock_post.return_value = mock_response

        data = {
            'slope_submit': 'true',
            'district': 'Gasabo',
            'sector': 'Kinyinya',
            'extraction_type': 'coordinates',
            'min_lon': '30.0', 'min_lat': '-2.0',
            'max_lon': '30.1', 'max_lat': '-1.9'
        }
        request = request_factory.post('/etl/dashboard/', data)
        request = setup_request(request, mock_admin_user)
        request.headers = {'X-Requested-With': 'XMLHttpRequest'}
        
        response = etl_dashboard(request)
        assert response.status_code == 200
        assert json.loads(response.content)['success'] is True

    @patch('app.etl_app.views.etl_dashboard_view.requests.post')
    def test_api_failure(self, mock_post, request_factory, mock_admin_user):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_post.return_value = mock_response

        data = {'malaria_submit': 'true', 'years': '2023', 'province': 'K', 'district': 'D'}
        request = request_factory.post('/etl/dashboard/', data)
        request = setup_request(request, mock_admin_user)
        request.headers = {'X-Requested-With': 'XMLHttpRequest'}
        
        response = etl_dashboard(request)
        assert response.status_code == 500
        assert json.loads(response.content)['success'] is False

    @patch('app.etl_app.views.etl_dashboard_view.requests.post')
    def test_validation_error(self, mock_post, request_factory, mock_admin_user):
        # Missing years
        data = {'malaria_submit': 'true', 'province': 'K', 'district': 'D'}
        request = request_factory.post('/etl/dashboard/', data)
        request = setup_request(request, mock_admin_user)
        # Use Standard POST (redirect)
        
        response = etl_dashboard(request)
        assert response.status_code == 302 # Redirects back on error
        
        # Verify message was added
        messages = list(request._messages)
        assert len(messages) > 0
        assert "Input error" in str(messages[0])

    @patch('app.etl_app.views.etl_dashboard_view.requests.post')
    def test_invalid_years_format(self, mock_post, request_factory, mock_admin_user):
        data = {
            'malaria_submit': 'true', 
            'province': 'Kigali', 
            'district': 'Gasabo',
            'years': '2022,abc' # Invalid format
        }
        request = request_factory.post('/etl/dashboard/', data)
        request = setup_request(request, mock_admin_user)
        
        response = etl_dashboard(request)
        assert response.status_code == 302
        messages = list(request._messages)
        assert "Years must be comma-separated numbers" in str(messages[0])

    def test_unknown_form_type(self, request_factory, mock_admin_user):
        data = {'unknown_submit': 'true'}
        request = request_factory.post('/etl/dashboard/', data)
        request = setup_request(request, mock_admin_user)
        
        response = etl_dashboard(request)
        assert response.status_code == 302
        messages = list(request._messages)
        assert "Could not identify form type" in str(messages[0])
