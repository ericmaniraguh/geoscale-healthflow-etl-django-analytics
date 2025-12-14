
import pytest
from unittest.mock import MagicMock, patch, ANY
from django.urls import reverse
from rest_framework.test import APIRequestFactory, force_authenticate
from rest_framework import status
from app.upload_app.views.SlopeGeoJsonUploadView import (
    GeoTiffUploadView, ShapefileUploadView, GeoJSONFetchView, 
    SlopeMetadataListView, SlopeDataSearchView
)
from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
import json
import io

@pytest.fixture
def factory():
    return APIRequestFactory()

@pytest.fixture
def user(db):
    User = get_user_model()
    return User.objects.create_user(username='testuser', password='password')

@pytest.mark.django_db
class TestSlopeGeoJsonUploadView:
    
    # ------------------------------------------------------------------------
    # GeoTiffUploadView Tests
    # ------------------------------------------------------------------------
    
    @patch('app.upload_app.views.SlopeGeoJsonUploadView.tif_to_geojson')
    @patch('app.upload_app.views.SlopeGeoJsonUploadView._get_mongo_collection')
    def test_geotiff_upload_success(self, mock_get_mongo, mock_tif_to_geojson, factory, user):
        # Mocks
        mock_tif_to_geojson.return_value = {
            "type": "FeatureCollection", 
            "features": [{"type": "Feature", "geometry": {}, "properties": {}}],
            "metadata": {}
        }
        
        mock_client = MagicMock()
        mock_coll = MagicMock()
        mock_get_mongo.return_value = (mock_client, mock_coll)
        
        # Request
        file_content = b"fake tif content"
        tif_file = SimpleUploadedFile("test.tif", file_content, content_type="image/tiff")
        
        view = GeoTiffUploadView.as_view()
        request = factory.post(
            '/api/upload/tif', 
            {'tif_file': tif_file, 'region': 'Rwanda', 'year': 2024}, 
            format='multipart'
        )
        force_authenticate(request, user=user)
        response = view(request)
        
        assert response.status_code == status.HTTP_201_CREATED
        assert "upload_id" in response.data
        mock_coll.insert_one.assert_called() # Should be called for data and metadata

    def test_geotiff_upload_missing_file(self, factory, user):
        view = GeoTiffUploadView.as_view()
        request = factory.post('/api/upload/tif', {'region': 'Rwanda'}, format='multipart')
        force_authenticate(request, user=user)
        response = view(request)
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "No TIF file" in str(response.data)

    def test_geotiff_upload_invalid_extension(self, factory, user):
        view = GeoTiffUploadView.as_view()
        txt_file = SimpleUploadedFile("test.txt", b"text", content_type="text/plain")
        request = factory.post('/api/upload/tif', {'tif_file': txt_file, 'region': 'Rwanda'}, format='multipart')
        force_authenticate(request, user=user)
        response = view(request)
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Expected .tif" in str(response.data)

    # ------------------------------------------------------------------------
    # ShapefileUploadView Tests
    # ------------------------------------------------------------------------

    @patch('app.upload_app.views.SlopeGeoJsonUploadView.shapefile_zip_to_geojson')
    @patch('app.upload_app.views.SlopeGeoJsonUploadView._get_mongo_collection')
    def test_shapefile_upload_success(self, mock_get_mongo, mock_shp_to_geojson, factory, user):
        mock_shp_to_geojson.return_value = {
            "type": "FeatureCollection", 
            "features": [{"type": "Feature", "geometry": {}, "properties": {}}],
            "metadata": {}
        }
        
        mock_client = MagicMock()
        mock_coll = MagicMock()
        mock_get_mongo.return_value = (mock_client, mock_coll)
        
        zip_file = SimpleUploadedFile("test.zip", b"fake zip", content_type="application/zip")
        
        view = ShapefileUploadView.as_view()
        request = factory.post(
            '/api/upload/shapefile', 
            {'shp_zip': zip_file, 'region': 'Rwanda'}, 
            format='multipart'
        )
        force_authenticate(request, user=user)
        response = view(request)
        
        assert response.status_code == status.HTTP_201_CREATED
        mock_shp_to_geojson.assert_called_once()
        mock_coll.insert_one.assert_called()

    # ------------------------------------------------------------------------
    # Fetch and List Views Tests
    # ------------------------------------------------------------------------

    @patch('app.upload_app.views.SlopeGeoJsonUploadView._get_mongo_collection')
    def test_geojson_fetch_found(self, mock_get_mongo, factory, user):
        mock_client = MagicMock()
        mock_coll = MagicMock()
        mock_get_mongo.return_value = (mock_client, mock_coll)
        
        mock_coll.find_one.return_value = {
            "_id": "fake_obj_id",
            "_upload_id": "u123",
            "geojson_data": {"type": "FeatureCollection", "features": []}
        }
        
        view = GeoJSONFetchView.as_view()
        request = factory.get('/api/geojson/u123')
        force_authenticate(request, user=user)
        response = view(request, upload_id="u123")
        
        assert response.status_code == 200
        assert response.data['upload_id'] == "u123"

    @patch('app.upload_app.views.SlopeGeoJsonUploadView._get_mongo_collection')
    def test_metadata_list(self, mock_get_mongo, factory, user):
        mock_client = MagicMock()
        mock_coll = MagicMock()
        mock_get_mongo.return_value = (mock_client, mock_coll)
        
        mock_coll.find.return_value = [
            {"_id": "id1", "dataset_name": "d1"}, 
            {"_id": "id2", "dataset_name": "d2"}
        ]
        
        view = SlopeMetadataListView.as_view()
        request = factory.get('/api/geojson/metadata')
        force_authenticate(request, user=user)
        response = view(request)
        
        assert response.status_code == 200
        assert response.data['total_datasets'] == 2
