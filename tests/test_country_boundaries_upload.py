
import pytest
import os
import json
import zipfile
import tempfile
import pandas as pd
import geopandas as gpd
from unittest.mock import MagicMock, patch, ANY
from django.urls import reverse
from rest_framework.test import APIRequestFactory, force_authenticate
from rest_framework import status
from app.upload_app.views.country_adm_boundaries_upload_views import (
    create_shapefile_collection_name,
    identify_village_name_column,
    detect_village_level_shapefile,
    UploadShapefileCountryView,
    ShapefileDataExtractionView,
    DeleteShapefileDatasetView,
    ShapefileMetadataListView
)

# -----------------------------------------------------------------------------
# Utility Function Tests
# -----------------------------------------------------------------------------

def test_create_shapefile_collection_name():
    name = create_shapefile_collection_name("Rwanda", "Villages", 2023)
    assert name == "shapefile-rwanda-villages-2023"

    name_special_chars = create_shapefile_collection_name("South Africa", "Admin-4", "2023")
    assert name_special_chars == "shapefile-southafrica-admin4-2023"

def test_identify_village_name_column():
    # Test valid village column
    df = pd.DataFrame({'Village': ['A', 'B'], 'id': [1, 2]})
    gdf = gpd.GeoDataFrame(df)
    assert identify_village_name_column(gdf) == 'Village'

    # Test fallback to "name"
    df = pd.DataFrame({'NAME_4': ['A', 'B'], 'ISO': ['RWA', 'RWA']})
    gdf = gpd.GeoDataFrame(df)
    assert identify_village_name_column(gdf) == 'NAME_4'

    # Test French fallback
    df = pd.DataFrame({'Nom_Village': ['A', 'B']})
    gdf = gpd.GeoDataFrame(df)
    assert identify_village_name_column(gdf) == 'Nom_Village'

    # Test no match
    df = pd.DataFrame({'pop': [100, 200], 'id': [1, 2]})
    gdf = gpd.GeoDataFrame(df)
    assert identify_village_name_column(gdf) is None

@patch('os.walk')
@patch('geopandas.read_file')
def test_detect_village_level_shapefile(mock_read_file, mock_walk):
    # Setup mock file system structure
    mock_walk.return_value = [
        ('/tmp/extract', [], ['admin1.shp', 'villages_2023.shp', 'readme.txt'])
    ]
    
    # Mock admin1.shp content (low feature count)
    mock_gdf_admin = MagicMock()
    mock_gdf_admin.empty = False
    mock_gdf_admin.columns = ['NAME_1', 'geometry']
    mock_gdf_admin.__len__.return_value = 5
    
    # Mock villages.shp content (high feature count + keyword column)
    mock_gdf_village = MagicMock()
    mock_gdf_village.empty = False
    mock_gdf_village.columns = ['Village_Name', 'geometry']
    mock_gdf_village.__len__.return_value = 1000
    
    # Side effect based on filename input to read_file
    def read_file_side_effect(path):
        if 'villages' in path:
            return mock_gdf_village
        return mock_gdf_admin
    
    mock_read_file.side_effect = read_file_side_effect

    selected_path, all_files = detect_village_level_shapefile('/tmp/extract')
    
    # Verify villages file was selected
    assert 'villages_2023.shp' in selected_path
    
    # Verify prioritization
    village_info = next(f for f in all_files if 'villages' in f['filename'])
    admin_info = next(f for f in all_files if 'admin1' in f['filename'])
    assert village_info['priority'] > admin_info['priority']

# -----------------------------------------------------------------------------
# Upload View Tests
# -----------------------------------------------------------------------------

@pytest.fixture
def api_factory():
    return APIRequestFactory()

@pytest.fixture
def mock_upload_file():
    # Create a dummy zip file
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp:
        with zipfile.ZipFile(tmp, 'w') as z:
            z.writestr('test.txt', 'dummy content')
        tmp_name = tmp.name
    
    from django.core.files.uploadedfile import SimpleUploadedFile
    with open(tmp_name, 'rb') as f:
        content = f.read()
            
    os.remove(tmp_name)
    return SimpleUploadedFile("test_upload.zip", content, content_type="application/zip")

class TestUploadShapefileCountryView:
    
    @patch('app.upload_app.views.country_adm_boundaries_upload_views.MongoClient')
    @patch('app.upload_app.views.country_adm_boundaries_upload_views.detect_village_level_shapefile')
    @patch('geopandas.read_file')
    @patch('zipfile.is_zipfile', return_value=True)
    def test_post_success(self, mock_is_zip, mock_read_file, mock_detect, mock_mongo, api_factory, mock_upload_file):
        # Setup Mongo Mock
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        
        mock_mongo.return_value = mock_client
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_db.list_collection_names.return_value = [] # No existing collections
        
        # Setup insert_many mock to return one inserted ID (matches our 1 feature mock)
        mock_collection.insert_many.return_value.inserted_ids = ['fake_object_id']
        
        # Setup Detect Mock
        mock_detect.return_value = ('/tmp/extracted/villages.shp', [])
        
        # Setup GeoPandas Mock
        mock_gdf = MagicMock()
        mock_gdf.__len__.return_value = 10
        mock_gdf.columns.tolist.return_value = ['Village', 'geometry']
        mock_gdf.geom_type.iloc.__getitem__.return_value = 'Polygon'
        mock_gdf.crs = 'EPSG:4326'
        # to_json needs to return a valid string representation of a dict
        mock_gdf.to_json.return_value = json.dumps({
            "type": "FeatureCollection",
            "features": [{"type": "Feature", "properties": {"Village": "V1"}, "geometry": {}}]
        })
        # Support iloc for iterating
        mock_gdf.iloc.__getitem__.return_value = 'V1' 
        
        def iloc_side_effect(idx):
             row_mock = MagicMock()
             row_mock.__getitem__.return_value = "VillageName"
             return row_mock
        mock_gdf.iloc.__getitem__.side_effect = iloc_side_effect

        mock_read_file.return_value = mock_gdf

        # Create Request
        view = UploadShapefileCountryView.as_view()
        data = {
            'file': mock_upload_file,
            'dataset_name': 'Test Dataset',
            'country': 'Rwanda',
            'year': '2023',
            'shapefile_type': 'villages'
        }
        request = api_factory.post('/upload/shapefile/', data, format='multipart')
        response = view(request)

        print(f"DEBUG: Response Status: {response.status_code}")
        print(f"DEBUG: Response Data: {response.data}")

        assert response.status_code == status.HTTP_201_CREATED
        assert response.data['message'] == 'Village-level shapefile uploaded and stored successfully.'
        assert response.data['features_inserted'] == 1 # Based on our mock geojson features list length (1)

        # Verify Metadata Insertion
        mock_db['shapefile-rwanda-villages-2023_metadata'].insert_one.assert_called_once()
        
        # Verify Data Helper calling insert_many
        # Since logic calls upload_geojson_to_mongo which calls insert_many
        # It's called on the data collection, not metadata
        mock_db[f'shapefile-{data["country"].lower()}-{data["shapefile_type"].lower()}-{data["year"]}'].insert_many.assert_called()

    def test_post_missing_metadata(self, api_factory, mock_upload_file):
        view = UploadShapefileCountryView.as_view()
        # Missing year and country
        data = {
            'file': mock_upload_file,
            'dataset_name': 'Test Dataset',
        }
        request = api_factory.post('/upload/shapefile/', data, format='multipart')
        response = view(request)
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert 'error' in response.data

    def test_post_invalid_file_extension(self, api_factory):
        view = UploadShapefileCountryView.as_view()
        from django.core.files.uploadedfile import SimpleUploadedFile
        file = SimpleUploadedFile("test.txt", b"content", content_type="text/plain")
        
        data = {'file': file}
        request = api_factory.post('/upload/shapefile/', data, format='multipart')
        response = view(request)
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST


# -----------------------------------------------------------------------------
# Data Extraction View Tests
# -----------------------------------------------------------------------------

class TestShapefileDataExtractionView:
    
    @patch('app.upload_app.views.country_adm_boundaries_upload_views.MongoClient')
    def test_get_filtered_data(self, mock_mongo, api_factory):
        # Setup Mock
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        
        mock_mongo.return_value = mock_client
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        
        # Mock Find Return
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = [
            {'_id': 'fake_id', '_village_name': 'Kigali Village', 'properties': {}}
        ]
        mock_collection.find.return_value = mock_cursor
        
        view = ShapefileDataExtractionView.as_view()
        request = api_factory.get('/data/shapefile/', {'country': 'Rwanda', 'year': '2023', 'shapefile_type': 'villages'})
        response = view(request)
        
        assert response.status_code == status.HTTP_200_OK
        assert response.data['total_features'] == 1
        
        # Verify correct collection was targeted based on params
        mock_db.__getitem__.assert_called_with('shapefile-rwanda-villages-2023')
        
    @patch('app.upload_app.views.country_adm_boundaries_upload_views.MongoClient')
    def test_get_all_collections_search(self, mock_mongo, api_factory):
        # Setup Mock to simulate multiple collections
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        
        mock_mongo.return_value = mock_client
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        
        mock_db.list_collection_names.return_value = ['shapefile-rwanda-2023', 'shapefile-uganda-2023', 'other_collection']
        
        view = ShapefileDataExtractionView.as_view()
        # No specific filters provided, should search all shapefile-* collections
        request = api_factory.get('/data/shapefile/')
        response = view(request)
        
        assert response.status_code == status.HTTP_200_OK
        # Should have searched 2 collections
        assert len(response.data['collections_searched']) == 2
        assert 'shapefile-rwanda-2023' in response.data['collections_searched']

# -----------------------------------------------------------------------------
# Delete View Tests
# -----------------------------------------------------------------------------

class TestDeleteShapefileDatasetView:
    
    @patch('app.upload_app.views.country_adm_boundaries_upload_views.MongoClient')
    def test_delete_success(self, mock_mongo, api_factory):
        # Setup Mock
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        
        mock_mongo.return_value = mock_client
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection # Generic return
        
        # Mock Metadata Search
        mock_db.list_collection_names.return_value = ['shapefile-test_metadata']
        
        # Metadata 'find_one' should return a match for the ID
        mock_metadata_col = MagicMock()
        mock_metadata_col.find_one.return_value = {'upload_id': 'test-uuid-123'}
        mock_metadata_col.delete_one.return_value.deleted_count = 1
        
        # Data collection mock
        mock_data_col = MagicMock()
        mock_data_col.delete_many.return_value.deleted_count = 50
        
        def getitem_side_effect(name):
            if 'metadata' in name:
                return mock_metadata_col
            return mock_data_col
        
        mock_db.__getitem__.side_effect = getitem_side_effect
        
        view = DeleteShapefileDatasetView.as_view()
        request = api_factory.delete('/delete/shapefile/test-uuid-123/')
        response = view(request, upload_id='test-uuid-123')
        
        assert response.status_code == status.HTTP_200_OK
        assert response.data['features_deleted'] == 50
        assert response.data['metadata_deleted'] == 1
        
    @patch('app.upload_app.views.country_adm_boundaries_upload_views.MongoClient')
    def test_delete_not_found(self, mock_mongo, api_factory):
        mock_client = MagicMock()
        mock_db = MagicMock()
        
        mock_mongo.return_value = mock_client
        mock_client.__getitem__.return_value = mock_db
        
        # No metadata match
        mock_db.list_collection_names.return_value = ['shapefile-test_metadata']
        mock_metadata_col = MagicMock()
        mock_metadata_col.find_one.return_value = None # Not found
        mock_db.__getitem__.return_value = mock_metadata_col
        
        view = DeleteShapefileDatasetView.as_view()
        request = api_factory.delete('/delete/shapefile/non-existent-id/')
        response = view(request, upload_id='non-existent-id')
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
