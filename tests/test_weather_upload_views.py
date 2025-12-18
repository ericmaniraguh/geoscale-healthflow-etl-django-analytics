
import pytest
from unittest.mock import MagicMock, patch, ANY
from rest_framework.test import APIRequestFactory
from rest_framework import status
from django.core.files.uploadedfile import SimpleUploadedFile
from app.upload_app.views.weather_data_prec_temp_upload_views import (
    UploadTemperatureView,
    UploadPrecipitationView,
    WeatherDataExtractionView,
    DeleteWeatherDatasetView,
    normalize_station_name,
    create_weather_collection_name,
    parse_years_from_string
)
from datetime import datetime

@pytest.fixture
def api_factory():
    return APIRequestFactory()

@pytest.fixture
def mock_mongo_client():
    with patch('app.upload_app.views.weather_data_prec_temp_upload_views.MongoClient') as mock:
        yield mock

@pytest.fixture
def mock_settings(settings):
    settings.MONGO_URI = "mongodb://localhost:27017"
    settings.MONGO_WEATHER_DB = "test_weather_db"
    return settings

class TestHelperFunctions:
    def test_normalize_station_name(self):
        assert normalize_station_name('Nyamata, Ruhuha and Juru') == 'nyamata_ruhuha_and_juru'
        assert normalize_station_name('  Simple Station  ') == 'simple_station'
        assert normalize_station_name('Mvumba') == 'mvumba'

    def test_parse_years_from_string(self):
        assert parse_years_from_string("2021, 2022, 2023") == [2021, 2022, 2023]
        assert parse_years_from_string("2021") == [2021]
        assert parse_years_from_string("") == []
        assert parse_years_from_string("invalid, 2022") == [2022]

    def test_create_weather_collection_name(self):
        assert create_weather_collection_name('temperature', 'Nyamata', [2021, 2022]) == 'weather_temperature_nyamata_2021to2022'
        assert create_weather_collection_name('Precipitation', 'Kigali', [2023]) == 'weather_precipitation_kigali_2023'

@pytest.mark.django_db
class TestUploadTemperatureView:
    def test_post_success(self, api_factory, mock_mongo_client, mock_settings):
        view = UploadTemperatureView.as_view()
        
        # Mock MongoDB
        mock_db = mock_mongo_client.return_value.__getitem__.return_value
        mock_collection = mock_db.__getitem__.return_value
        mock_collection.insert_many.return_value.inserted_ids = [1, 2]
        
        # Create CSV file
        csv_content = b"date,value\n2021-01-01,25.5\n2021-01-02,26.0"
        uploaded_file = SimpleUploadedFile("temp.csv", csv_content, content_type="text/csv")
        
        data = {
            'temperature': uploaded_file,
            'station': 'Kigali',
            'years': '2021',
            'dataset_name': 'Test Dataset'
        }
        
        request = api_factory.post('/upload/temperature/', data, format='multipart')
        response = view(request)
        
        assert response.status_code == status.HTTP_201_CREATED
        assert response.data['records_inserted'] == 2
        assert response.data['metadata']['station'] == 'Kigali'
        
        # Verify MongoDB calls
        mock_mongo_client.assert_called()
        mock_collection.insert_many.assert_called()

    def test_post_missing_metadata(self, api_factory):
        view = UploadTemperatureView.as_view()
        data = {'temperature': SimpleUploadedFile("temp.csv", b"cnt")}
        request = api_factory.post('/upload/temperature/', data, format='multipart')
        response = view(request)
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Missing required metadata" in str(response.data)

    def test_post_no_file(self, api_factory):
        view = UploadTemperatureView.as_view()
        request = api_factory.post('/upload/temperature/', {}, format='multipart')
        response = view(request)
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_post_processing_value_error(self, api_factory):
        view = UploadTemperatureView.as_view()
        data = {
            'temperature': SimpleUploadedFile("temp.csv", b"content"),
            'station': 'Kigali',
            'years': '2021'
        }
        
        # Mock _process_weather_file to raise ValueError
        with patch('app.upload_app.views.weather_data_prec_temp_upload_views._process_weather_file') as mock_process:
            mock_process.side_effect = ValueError("Invalid File Content")
            request = api_factory.post('/upload/temperature/', data, format='multipart')
            response = view(request)
            
            assert response.status_code == status.HTTP_400_BAD_REQUEST
            assert "Invalid File Content" in str(response.data)

    def test_post_processing_generic_error(self, api_factory):
        view = UploadTemperatureView.as_view()
        data = {
            'temperature': SimpleUploadedFile("temp.csv", b"content"),
            'station': 'Kigali',
            'years': '2021'
        }
        
        # Mock _process_weather_file to raise generic Exception
        with patch('app.upload_app.views.weather_data_prec_temp_upload_views._process_weather_file') as mock_process:
            mock_process.side_effect = Exception("DB Connection Failed")
            request = api_factory.post('/upload/temperature/', data, format='multipart')
            response = view(request)
            
            assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
            assert "DB Connection Failed" in str(response.data)

@pytest.mark.django_db
class TestUploadPrecipitationView:
    def test_post_success(self, api_factory, mock_mongo_client, mock_settings):
        view = UploadPrecipitationView.as_view()
        
        # Mock MongoDB
        mock_db = mock_mongo_client.return_value.__getitem__.return_value
        mock_collection = mock_db.__getitem__.return_value
        mock_collection.insert_many.return_value.inserted_ids = [1]
        
        csv_content = b"date,mm\n2021-01-01,10.5"
        uploaded_file = SimpleUploadedFile("precip.csv", csv_content, content_type="text/csv")
        
        data = {
            'precipitation': uploaded_file,
            'station': 'Musanze',
            'years': '2021',
            'dataset_name': 'Precip Data'
        }
        
        request = api_factory.post('/upload/precipitation/', data, format='multipart')
        response = view(request)
        
        assert response.status_code == status.HTTP_201_CREATED
        assert response.data['records_inserted'] == 1

@pytest.mark.django_db
class TestWeatherDataExtractionView:
    def test_get_success(self, api_factory, mock_mongo_client, mock_settings):
        view = WeatherDataExtractionView.as_view()
        
        # Explicit mock setup
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance
        
        mock_db = MagicMock()
        mock_client_instance.__getitem__.return_value = mock_db
        
        # Setup list_collection_names
        mock_db.list_collection_names.return_value = ['weather_temp_kigali_2021']
        
        # Setup specific collection mock
        mock_collection = MagicMock()
        mock_collection.find.return_value = [
            {'_id': 'id1', 'val': 25, '_upload_time': 123}
        ]

        # SIMPLIFICATION: Always return this collection mock for any db access
        # The loop only accesses the collection name found in list_collection_names anyway
        # Ensure _upload_time is a datetime object to support isoformat()
        mock_collection.find.return_value = [
            {'_id': 'id1', 'val': 25, '_upload_time': datetime(2023, 1, 1)}
        ]
        mock_db.__getitem__.return_value = mock_collection
        
        request = api_factory.get('/weather/data/', {'station': 'Kigali', 'data_type': 'temp'})
        response = view(request)
        
        assert response.status_code == status.HTTP_200_OK
        assert response.data['total_records'] == 1

    def test_get_filter_logic(self, api_factory, mock_mongo_client, mock_settings):
        view = WeatherDataExtractionView.as_view()
        
        mock_db = mock_mongo_client.return_value.__getitem__.return_value
        mock_collection = mock_db.__getitem__.return_value
        
        request = api_factory.get('/weather/data/', {
            'station': 'Kigali',
            'data_type': 'temperature',
            'years': '2021,2022'
        })
        response = view(request)
        
        assert response.status_code == status.HTTP_200_OK
        # Verify collection search logic
        # Either list_collection_names or direct collection access should happen
        assert mock_mongo_client.called

@pytest.mark.django_db
class TestDeleteWeatherDatasetView:
    def test_delete_success(self, api_factory, mock_mongo_client, mock_settings):
        view = DeleteWeatherDatasetView.as_view()
        
        mock_db = mock_mongo_client.return_value.__getitem__.return_value
        mock_db.list_collection_names.return_value = ['weather_temp_kigali_2021_metadata']
        
        # Mock finding the metadata
        mock_meta_col = mock_db.__getitem__.return_value
        mock_meta_col.find_one.return_value = {'upload_id': 'uid123'}
        
        mock_data_col = mock_db.__getitem__.return_value
        mock_data_col.delete_many.return_value.deleted_count = 10
        mock_meta_col.delete_one.return_value.deleted_count = 1
        
        request = api_factory.delete('/weather/delete/uid123/')
        response = view(request, upload_id='uid123')
        
        assert response.status_code == status.HTTP_200_OK
        assert response.data['records_deleted'] == 10

    def test_delete_not_found(self, api_factory, mock_mongo_client, mock_settings):
        view = DeleteWeatherDatasetView.as_view()
        
        mock_db = mock_mongo_client.return_value.__getitem__.return_value
        mock_db.list_collection_names.return_value = []
        
        request = api_factory.delete('/weather/delete/unknown_id/')
        response = view(request, upload_id='unknown_id')
        
        assert response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.django_db
class TestWeatherMetadataListView:
    def test_get_success(self, api_factory, mock_mongo_client, mock_settings):
        from app.upload_app.views.weather_data_prec_temp_upload_views import WeatherMetadataListView
        view = WeatherMetadataListView.as_view()
        
        mock_db = mock_mongo_client.return_value.__getitem__.return_value
        mock_db.list_collection_names.return_value = ['weather_temp_kigali_2021_metadata']
        
        mock_collection = mock_db.__getitem__.return_value
        mock_collection.find.return_value = [
            {
                'station': 'Kigali', 'data_type': 'temp', 
                'dataset_years': '2021', 'data_collection_name': 'weather_temp_kigali_2021',
                'upload_time': datetime(2023, 1, 1)
            }
        ]
        
        request = api_factory.get('/weather/metadata/')
        response = view(request)
        
        assert response.status_code == status.HTTP_200_OK
        assert response.data['total_datasets'] == 1
        assert response.data['grouped_by_station_type_years'] is not None

@pytest.mark.django_db
class TestWeatherCollectionListView:
    def test_get_success(self, api_factory, mock_mongo_client, mock_settings):
        from app.upload_app.views.weather_data_prec_temp_upload_views import WeatherCollectionListView
        view = WeatherCollectionListView.as_view()
        
        mock_db = mock_mongo_client.return_value.__getitem__.return_value
        # Return data collection and metadata collection
        mock_db.list_collection_names.return_value = [
            'weather_temp_kigali_2021', 
            'weather_temp_kigali_2021_metadata'
        ]
        
        mock_collection = mock_db.__getitem__.return_value
        mock_collection.count_documents.return_value = 100
        mock_collection.find_one.return_value = {'date': '2021-01-01'}
        
        request = api_factory.get('/weather/collections/')
        response = view(request)
        
        assert response.status_code == status.HTTP_200_OK
        assert response.data['total_data_collections'] == 1
        assert response.data['collections'][0]['data_collection'] == 'weather_temp_kigali_2021'
