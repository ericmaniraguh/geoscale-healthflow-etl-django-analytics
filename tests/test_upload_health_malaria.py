
import pytest
from unittest.mock import MagicMock, patch, ANY
import pandas as pd
from django.core.files.uploadedfile import SimpleUploadedFile
from rest_framework.test import APIRequestFactory
from rest_framework import status
from app.upload_app.views.health_center_lab__data_upload_views import (
    UploadHealthCenterLabDataView, DataExtractionView, MetadataListView, DeleteDatasetView
)
from app.upload_app.views.malaria_htmis_api_upload_view import (
    UploadHMISAPIDataView, HMISDataExtractionView, HMISMetadataListView, DeleteHMISDatasetView
)

@pytest.fixture
def factory():
    return APIRequestFactory()

@pytest.mark.django_db
class TestHealthCenterUpload:
    @patch('app.upload_app.views.health_center_lab__data_upload_views.MongoClient')
    @patch('app.upload_app.views.health_center_lab__data_upload_views.pd.read_csv')
    def test_upload_csv_success(self, mock_read_csv, mock_mongo, factory):
        # Mock DataFrame
        mock_df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        mock_read_csv.return_value = mock_df
        
        # Mock Mongo
        # Configure the patch object directly
        mock_client_instance = MagicMock()
        mock_mongo.return_value = mock_client_instance
        
        mock_db = MagicMock()
        mock_client_instance.__getitem__.return_value = mock_db
        
        mock_collection = MagicMock()
        mock_collection.insert_many.return_value.inserted_ids = [1, 2]
        mock_db.__getitem__.return_value = mock_collection
        
        file = SimpleUploadedFile("test.csv", b"col1,col2\n1,3\n2,4", content_type="text/csv")
        
        view = UploadHealthCenterLabDataView.as_view()
        request = factory.post(
            '/api/upload/health-center',
            {
                'file': file,
                'dataset_name': 'Test HC',
                'district': 'Gasabo',
                'sector': 'Kacyiru',
                'health_center': 'HC1',
                'year': 2024
            },
            format='multipart'
        )
        
        response = view(request)
        
        assert response.status_code == status.HTTP_201_CREATED
        assert response.data['records_inserted'] == 2
        mock_read_csv.assert_called()
        mock_collection.insert_many.assert_called()

    @patch('app.upload_app.views.health_center_lab__data_upload_views.MongoClient')
    def test_data_extraction(self, mock_mongo, factory):
        mock_db = MagicMock()
        mock_mongo.return_value.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = [{'_id': '1', 'data': 'val'}]
        mock_collection.find.return_value = mock_cursor
        mock_db.__getitem__.return_value = mock_collection # Default collection return
        # Mock list_collection_names for wildcard search
        mock_db.list_collection_names.return_value = ['healthcenter-data-gasabo-kacyiru-2024']

        view = DataExtractionView.as_view()
        request = factory.get('/api/extract', {'district': 'Gasabo', 'year': 2024})
        response = view(request)
        
        assert response.status_code == 200
        assert len(response.data['data']) > 0

    @patch('app.upload_app.views.health_center_lab__data_upload_views.MongoClient')
    def test_metadata_list(self, mock_mongo, factory):
        mock_db = MagicMock()
        mock_mongo.return_value.__getitem__.return_value = mock_db
        mock_db.list_collection_names.return_value = ['healthcenter-data-x_metadata']
        
        mock_collection = MagicMock()
        mock_collection.find.return_value = [{'upload_id': 'u1', 'dataset_name': 'd1'}]
        mock_db.__getitem__.return_value = mock_collection
        
        view = MetadataListView.as_view()
        request = factory.get('/api/metadata')
        response = view(request)
        
        assert response.status_code == 200
        assert response.data['total_datasets'] == 1

    @patch('app.upload_app.views.health_center_lab__data_upload_views.MongoClient')
    def test_delete_dataset(self, mock_mongo, factory):
        mock_db = MagicMock()
        mock_mongo.return_value.__getitem__.return_value = mock_db
        mock_db.list_collection_names.return_value = ['healthcenter-data-x_metadata']
        
        mock_meta_coll = MagicMock()
        # Find upload_id in this collection
        mock_meta_coll.find_one.return_value = {'upload_id': 'u1'}
        mock_meta_coll.delete_one.return_value.deleted_count = 1
        
        mock_data_coll = MagicMock()
        mock_data_coll.delete_many.return_value.deleted_count = 10
        
        def getitem(name):
            if name == 'healthcenter-data-x_metadata': return mock_meta_coll
            if name == 'healthcenter-data-x': return mock_data_coll
            return MagicMock()
        mock_db.__getitem__.side_effect = getitem
        
        view = DeleteDatasetView.as_view()
        request = factory.delete('/api/delete/u1')
        response = view(request, upload_id='u1')
        
        assert response.status_code == 200
        assert response.data['message'] == "Dataset deleted successfully"


class TestHMISUpload:
    @patch('app.upload_app.views.malaria_htmis_api_upload_view.MongoClient')
    @patch('app.upload_app.views.malaria_htmis_api_upload_view.pd.read_excel')
    def test_upload_excel_success(self, mock_read_excel, mock_mongo, factory):
        mock_df = pd.DataFrame({'colA': [10, 20]})
        mock_read_excel.return_value = mock_df
        
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db # For settings.MONGO_HMIS_DB call logic
        # Ideally mock MongoClient(uri)[db_name]
        mock_mongo.return_value = mock_client
        
        mock_collection = MagicMock()
        mock_collection.insert_many.return_value.inserted_ids = [100, 101]
        mock_db.__getitem__.return_value = mock_collection
        
        file = SimpleUploadedFile("test.xlsx", b"fake excel", content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        
        view = UploadHMISAPIDataView.as_view()
        request = factory.post(
            '/api/upload/hmis',
            {
                'file': file,
                'dataset_name': 'Test HMIS',
                'district': 'Kicukiro',
                'health_facility': 'HF1',
                'year': 2024
            },
            format='multipart'
        )
        
        response = view(request)
        
        assert response.status_code == status.HTTP_201_CREATED
        assert response.data['records_inserted'] == 2

    @patch('app.upload_app.views.malaria_htmis_api_upload_view.MongoClient')
    def test_hmis_extraction(self, mock_mongo, factory):
        mock_db = MagicMock()
        mock_mongo.return_value.__getitem__.return_value = mock_db
        mock_db.list_collection_names.return_value = ['hmis-data-coll']
        
        mock_collection = MagicMock()
        mock_collection.find.return_value = [{'_id': '1', 'val': 10}]
        mock_db.__getitem__.return_value = mock_collection
        
        view = HMISDataExtractionView.as_view()
        request = factory.get('/api/extract')
        response = view(request)
        
        assert response.status_code == 200
        assert len(response.data['data']) > 0

    @patch('app.upload_app.views.malaria_htmis_api_upload_view.MongoClient')
    def test_delete_hmis_dataset(self, mock_mongo, factory):
        mock_db = MagicMock()
        mock_mongo.return_value.__getitem__.return_value = mock_db
        mock_db.list_collection_names.return_value = ['hmis-data-x_metadata']
        
        mock_meta_coll = MagicMock()
        mock_meta_coll.find_one.return_value = {'upload_id': 'h1'}
        
        def getitem(name):
             if name.endswith('_metadata'): return mock_meta_coll
             return MagicMock() # data collection
        mock_db.__getitem__.side_effect = getitem

        view = DeleteHMISDatasetView.as_view()
        request = factory.delete('/api/delete/h1')
        response = view(request, upload_id='h1')
        
        assert response.status_code == 200
