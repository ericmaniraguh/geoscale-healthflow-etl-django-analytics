import pytest
from django.urls import reverse
from django.contrib.auth import get_user_model

@pytest.mark.django_db
class TestAuth:
    def test_signup_view_get(self, client):
        """Test that the signup page loads successfully."""
        url = reverse('accounts:signup')
        response = client.get(url)
        assert response.status_code == 200

    def test_signup_view_post_success(self, client, user_data):
        """Test valid signup initiates OTP flow."""
        # Create required location data
        from app.accounts.models import Province, District, Sector
        province = Province.objects.create(name="Kigali City")
        district = District.objects.create(name="Gasabo", province=province)
        sector = Sector.objects.create(name="Kacyiru", district=district)

        url = reverse('accounts:signup')
        # Ensure password confirmation matches
        data = user_data.copy()
        data.update({
            'first_name': 'Test',
            'last_name': 'User',
            'position': 'Analyst',
            'terms_accepted': True,
            'password1': user_data['password'],
            'password2': user_data['password'],
            'country': 'Rwanda',
            'province': province.id,
            'district': district.id,
            'sector': sector.id
        })
        
        response = client.post(url, data)
        
        # Check for form errors if response is 200 (failure)
        if response.status_code == 200:
            raise Exception(f"Form errors: {response.context['form'].errors}")

        # User should NOT be created yet
        User = get_user_model()
        assert not User.objects.filter(username=user_data['username']).exists()
        
        # Should redirect to OTP verification
        assert response.status_code == 302
        assert response.url == reverse('accounts:verify_otp')

    def test_login_view_get(self, client):
        """Test that the login page loads successfully."""
        url = reverse('accounts:login')
        response = client.get(url)
        assert response.status_code == 200

    def test_login_view_post_success(self, client, create_user, user_data):
        """Test valid login."""
        # Create a user with email_verified=True
        create_user(email_verified=True) 
        
        url = reverse('accounts:login')
        response = client.post(url, {
            'username': user_data['username'], 
            'password': user_data['password']
        })
        assert response.status_code == 302 # Should redirect
        
        # Check if user is authenticated
        assert '_auth_user_id' in client.session

    def test_logout_view(self, authenticated_client):
        """Test logout functionality."""
        client, user = authenticated_client
        url = reverse('accounts:logout')
        response = client.post(url) # Logout usually requires POST or GET depending on config
        
        if response.status_code == 405: # Method Not Allowed (some setups require POST)
             response = client.get(url)
             
        assert response.status_code == 302 # Redirect after logout
        assert '_auth_user_id' not in client.session
