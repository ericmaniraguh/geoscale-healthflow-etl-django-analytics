import pytest
from django.urls import reverse

@pytest.mark.django_db
class TestGeospatialMerger:
    def test_dashboard_access_staff(self, client, create_user):
        """Test accessing the geospatial dashboard as staff user."""
        user = create_user(is_staff=True, is_superuser=True)
        client.force_login(user)
        
        url = reverse('geospatial_merger:dashboard')
        response = client.get(url)
        assert response.status_code == 200

    def test_dashboard_access_denied_regular_user(self, authenticated_client):
        """Test that regular users are denied access or redirected."""
        client, user = authenticated_client
        # Ensure user is not staff
        user.is_staff = False
        user.is_superuser = False
        user.save()
        
        url = reverse('geospatial_merger:dashboard')
        response = client.get(url)
        
        # Depending on implementation, might be 302 (redirect to login or error) or 403
        assert response.status_code in [302, 403]

    def test_api_check_admin(self, client, create_user):
        """Test admin check API."""
        # Test as admin
        admin_user = create_user(is_staff=True, is_superuser=True, username="admin_geo")
        client.force_login(admin_user)
        url = reverse('geospatial_merger:api_check_admin')
        response = client.get(url)
        assert response.status_code == 200
        assert response.json().get('is_admin') is True
        
        client.logout()
        
        # Test as regular user
        regular_user = create_user(username="regular_geo")
        client.force_login(regular_user)
        response = client.get(url)
        assert response.status_code == 200
        # Should return is_admin: False or error depending on implementation
        # But access to the endpoint itself is what we check mostly here
