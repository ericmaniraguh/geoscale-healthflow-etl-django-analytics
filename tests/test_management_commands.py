import pytest
from django.core.management import call_command
from django.core import mail
from unittest.mock import patch, MagicMock
from app.accounts.models import CustomUser

@pytest.mark.django_db
class TestEmailCommand:
    def test_send_test_email(self):
        """Test the sending of a test email."""
        with patch('app.accounts.views.send_otp_email_simple', return_value=True):
            call_command('test_email', to='test@example.com')
        
        # Check outbox for the plain email sent by send_mail
        assert len(mail.outbox) >= 1
        assert mail.outbox[0].subject == 'Test Email from GeoScale Malaria Platform'
        assert 'test@example.com' in mail.outbox[0].to

@pytest.mark.django_db
class TestCreateSuperuserSignal:
    def test_superuser_verification(self):
        """Test that created superusers are automatically verified (signal check)."""
        # The file name is confusingly app/management/commands/createsuperuser.py, 
        # but it contains a signal. We assume this signal is connected when the app loads.
        # We test the behavior (creating superuser -> verified=True)
        
        admin_user = CustomUser.objects.create_superuser(
            username='auto_admin', 
            email='admin@example.com', 
            password='AdminPass123!'
        )
        assert admin_user.is_superuser
        assert admin_user.email_verified is True

@pytest.mark.django_db
class TestSendActivationEmailsCommand:
    def test_send_activation_dry_run(self):
        """Test dry run mode."""
        CustomUser.objects.create_user(username='u1', email='u1@example.com', password='p1', email_verified=False)
        
        # Capture stdout to verify messages if needed, but for now we look at mail.outbox
        call_command('send_activation_emails', dry_run=True)
        assert len(mail.outbox) == 0

    def test_send_activation_real(self):
        """Test actual sending."""
        user = CustomUser.objects.create_user(username='u2', email='u2@example.com', password='p2', email_verified=False)
        
        call_command('send_activation_emails')
        assert len(mail.outbox) == 1
        assert 'Activate Your Account' in mail.outbox[0].subject
        assert user.email in mail.outbox[0].to

    def test_send_activation_specific_user(self):
        """Test sending to a specific user ID."""
        user = CustomUser.objects.create_user(username='u3', email='u3@example.com', password='p3', email_verified=False)
        
        call_command('send_activation_emails', user_id=user.id)
        assert len(mail.outbox) == 1
        assert user.email in mail.outbox[0].to

class TestConnectionsCommand:
    @patch('django.db.connection.cursor')
    def test_test_connections_postgres(self, mock_cursor):
        """Test PostgreSQL connection check."""
        # Mock cursor for version check
        mock_cursor.return_value.__enter__.return_value.fetchone.return_value = ['PostgreSQL 14.5']
        
        # We patch print/stdout to avoid clutter, or just let it run
        call_command('test_connections', postgresql=True)
        # If no exception raised, pass

    @patch('pymongo.MongoClient')
    @patch('mongoengine.connection.get_db')
    def test_test_connections_mongo(self, mock_get_db, mock_mongo_client):
        """Test MongoDB connection check."""
        mock_get_db.return_value.name = 'test_mongo_db'
        mock_mongo_client.return_value.list_database_names.return_value = ['admin', 'local']
        
        call_command('test_connections', mongodb=True)
        # If no exception raised, pass

    @patch('django.db.connection.cursor')
    def test_test_connections_all(self, mock_cursor):
        """Test running without arguments (tests all)."""
        mock_cursor.return_value.__enter__.return_value.fetchone.return_value = ['PostgreSQL 14.5']
        
        with patch('pymongo.MongoClient'), patch('mongoengine.connection.get_db'):
            call_command('test_connections')
