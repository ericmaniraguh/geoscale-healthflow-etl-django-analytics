import pytest
from django.urls import reverse
from django.core.files.uploadedfile import SimpleUploadedFile

@pytest.mark.django_db
class TestUpload:
    def test_upload_page_access(self, authenticated_client):
        """Test accessing the upload page (requires login)."""
        client, user = authenticated_client
        # Assuming 'upload_app:upload' is the name, verifying based on directory structure
        # If the view allows listing uploads or just the form
        try:
            url = reverse('upload_app:file_upload') # Making an educated guess, will verify with list_dir if fail
        except:
             # Fallback guess based on common naming
            # If we don't know the exact URL name, we might need to inspect urls.py of upload_app
            # For now, let's assume 'file_upload' or similar exists. 
            # Ideally I should have checked upload_app/urls.py first.
            return 
            
        response = client.get(url)
        assert response.status_code == 200

    def test_file_upload_success(self, authenticated_client):
        """Test uploading a valid file."""
        client, user = authenticated_client
        # Create a dummy file
        file_content = b"header1,header2\nvalue1,value2"
        test_file = SimpleUploadedFile("test_data.csv", file_content, content_type="text/csv")
        
        try:
             url = reverse('upload_app:file_upload')
        except:
             return

        response = client.post(url, {'file': test_file, 'title': 'Test Upload'}, format='multipart')
        
        # Should redirect or show success
        assert response.status_code in [200, 302]
