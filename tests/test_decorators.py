import pytest
from django.contrib.auth.models import AnonymousUser
from django.test import RequestFactory
from django.http import HttpResponse
from django.contrib.messages.storage.fallback import FallbackStorage
from app.decorators import admin_required, admin_required_class

@pytest.fixture
def request_factory():
    return RequestFactory()

@pytest.fixture
def mock_view():
    def view(request):
        return HttpResponse("Success")
    return view

@pytest.mark.django_db
class TestDecorators:
    def test_admin_required_anonymous(self, request_factory, mock_view):
        """Test admin_required redirects anonymous users."""
        decorated_view = admin_required(mock_view)
        request = request_factory.get('/')
        request.user = AnonymousUser()
        
        response = decorated_view(request)
        assert response.status_code == 302
        assert 'login' in response.url

    def test_admin_required_regular_user(self, request_factory, mock_view, create_user):
        """Test admin_required redirects regular users."""
        user = create_user(username='reg_user', email='reg@example.com', password='p')
        decorated_view = admin_required(mock_view)
        request = request_factory.get('/')
        request.user = user
        
        # Add message support
        setattr(request, 'session', 'session')
        messages = FallbackStorage(request)
        setattr(request, '_messages', messages)
        
        response = decorated_view(request)
        assert response.status_code == 302
        assert '/upload/dashboard/' in response.url # Should verify exact redirect target in decorator

    def test_admin_required_admin_user(self, request_factory, mock_view, create_user):
        """Test admin_required allows staff/superuser."""
        user = create_user(username='staff_user', email='staff@example.com', password='p', is_staff=True)
        decorated_view = admin_required(mock_view)
        request = request_factory.get('/')
        request.user = user
        
        response = decorated_view(request)
        assert response.status_code == 200
        assert response.content == b"Success"

    def test_admin_required_class_decorator(self, request_factory, create_user):
        """Test admin_required_class decorator."""
        from django.views import View
        
        @admin_required_class
        class AdminView(View):
            def get(self, request):
                return HttpResponse("Class Success")
        
        # Test valid admin
        user = create_user(username='class_admin', email='ca@example.com', password='p', is_superuser=True)
        request = request_factory.get('/')
        request.user = user
        
        view = AdminView.as_view()
        response = view(request)
        assert response.status_code == 200
        assert response.content == b"Class Success"
