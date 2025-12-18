from unittest.mock import MagicMock, patch
import pytest
from app.etl_app.services.health_center_mongodb_service import HealthCenterMongoDBService


class TestHealthCenterMongoDBService:

    @pytest.fixture
    def mock_mongo(self):
        with patch(
            "app.etl_app.services.health_center_mongodb_service.MongoClient"
        ) as mock_client:

            mock_db = MagicMock()
            mock_collection = MagicMock()

            # client[db]
            mock_client.return_value.__getitem__.return_value = mock_db
            # db[collection]
            mock_db.__getitem__.return_value = mock_collection

            yield mock_client, mock_db, mock_collection

    def test_get_available_filters(self, mock_mongo):
        _, mock_db, mock_collection = mock_mongo
        service = HealthCenterMongoDBService()

        mock_db.list_collection_names.return_value = [
            "healthcenter-data-1",
            "other",
        ]

        mock_collection.distinct.side_effect = lambda field: (
            [2023] if "year" in field.lower()
            else ["Bugesera"] if "district" in field.lower()
            else []
        )

        mock_collection.count_documents.return_value = 10
        mock_collection.find_one.return_value = {"year": 2023}

        filters = service.get_available_filters()

        assert 2023 in filters["years"]
        assert "Bugesera" in filters["districts"]

    def test_extract_data_for_analytics(self, mock_mongo):
        _, mock_db, mock_collection = mock_mongo
        service = HealthCenterMongoDBService()

        mock_db.list_collection_names.return_value = ["healthcenter-data-1"]

        mock_cursor = MagicMock()
        mock_cursor.batch_size.return_value = mock_cursor
        mock_cursor.__iter__.return_value = [
            {"_id": "123", "year": 2023}
        ]

        mock_collection.find.return_value = mock_cursor

        data = service.extract_data_for_analytics(years=[2023])

        assert len(data) == 1
        assert data[0]["year"] == 2023
        assert "_id" not in data[0]
