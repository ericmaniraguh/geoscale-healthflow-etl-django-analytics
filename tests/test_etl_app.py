import pytest
from django.urls import reverse

@pytest.mark.django_db
class TestETLApp:
    def test_etl_dashboard_access_authenticated(self, authenticated_client):
        """Test accessing the ETL dashboard."""
        client, user = authenticated_client
        url = reverse('etl_app:etl_dashboard')
        response = client.get(url)
        assert response.status_code == 200

    def test_etl_dashboard_redirect_unauthenticated(self, client):
        """Test that unauthenticated users are redirected."""
        url = reverse('etl_app:etl_dashboard')
        response = client.get(url)
        assert response.status_code == 302
        assert '/accounts/login' in response.url

    def test_etl_api_endpoints(self, authenticated_client):
        """Test ETL API endpoints availability."""
        client, user = authenticated_client
        # These are POST endpoints usually, or trigger processes
        # Checking if they exist and are protected
        
        endpoints = [
            'etl_app:malaria_api_calculator',
            'etl_app:hc-lab-etl',
            'etl_app:weather-data-etl',
        ]
        
        for endpoint in endpoints:
            url = reverse(endpoint)
            # GET might be not allowed (405) or return info, but should be accessible (not 404)
            response = client.get(url)
            assert response.status_code != 404, f"Endpoint {endpoint} not found"
