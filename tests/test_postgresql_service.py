import pytest
from unittest.mock import MagicMock, patch, ANY
from app.etl_app.services.postgresql_service import PostgreSQLService

class TestPostgreSQLService:

    @pytest.fixture
    def mock_engine_setup(self):
        with patch('app.etl_app.services.postgresql_service.create_engine') as mock_create_engine, \
             patch('app.etl_app.services.postgresql_service.settings') as mock_settings:
            
            # Setup mock settings
            mock_settings.DATABASES = {
                'default': {
                    'USER': 'user',
                    'PASSWORD': 'password',
                    'HOST': 'localhost',
                    'PORT': '5432',
                    'NAME': 'test_db'
                }
            }

            # Setup mock engine and connection
            mock_engine = MagicMock()
            mock_connection = MagicMock()
            mock_create_engine.return_value = mock_engine
            mock_engine.begin.return_value.__enter__.return_value = mock_connection
            
            yield mock_create_engine, mock_engine, mock_connection

    def test_connection_success(self, mock_engine_setup):
        _, _, mock_connection = mock_engine_setup
        service = PostgreSQLService()
        
        # Test connection check
        assert service._test_connection() is True
        mock_connection.execute.assert_called()

    def test_save_raw_data_replace(self, mock_engine_setup):
        _, _, mock_connection = mock_engine_setup
        service = PostgreSQLService()
        
        data = [{'unique_id': '1', 'year': 2023, 'district': 'Test'}]
        
        # Configure count return for final verification
        mock_connection.execute.return_value.fetchone.return_value = [1]
        
        success, message = service.save_raw_data(data, table_name='test_table', update_mode='replace')
        
        assert success is True
        # Verify separate calls were made (DROP, CREATE, INSERT, COUNT)
        assert mock_connection.execute.call_count >= 4 

    def test_save_raw_data_append(self, mock_engine_setup):
        _, _, mock_connection = mock_engine_setup
        service = PostgreSQLService()
        
        data = [{'unique_id': '1', 'year': 2023, 'district': 'Test'}]
        
        # Returns:
        # Prequel: SELECT 1 (from self.engine property access check 1)
        # Prequel: SELECT 1 (from self.engine property access check 2)
        # 1. CREATE TABLE
        # 2. UPSERT
        # 3. COUNT
        
        mock_result_dummy = MagicMock() # For SELECT 1
        mock_result_create = MagicMock()
        
        mock_result_upsert = MagicMock()
        mock_result_upsert.fetchone.return_value = [True]
        
        mock_result_count = MagicMock()
        mock_result_count.fetchone.return_value = [1]
        
        mock_connection.execute.side_effect = [
            mock_result_dummy, # valid check 1
            mock_result_dummy, # valid check 2
            mock_result_create,
            mock_result_upsert,
            mock_result_count
        ]
        
        success, message = service.save_raw_data(data, table_name='test_table', update_mode='append')
        
        assert success is True
        assert "inserted" in message

    def test_save_analytics(self, mock_engine_setup):
        _, _, mock_connection = mock_engine_setup
        service = PostgreSQLService()
        
        analytics_data = {
            'yearly_slide_status': [{'year': 2023, 'total': 10}],
            'gender_positivity_by_year': [{'year': 2023, 'gender': 'Male', 'total': 5}]
        }
        
        # Mock returns for multiple executes
        # We use a default return_value to handle the many SELECT 1 checks and queries
        mock_default_result = MagicMock()
        mock_default_result.fetchone.return_value = [1] 
        mock_connection.execute.return_value = mock_default_result
        mock_connection.execute.side_effect = None
        
        success, results = service.save_analytics(analytics_data)
        
        assert success is True
        assert 'yearly_slide_status' in results
        assert 'Saved to' in results['yearly_slide_status']

    def test_save_boundaries_data(self, mock_engine_setup):
        _, _, mock_connection = mock_engine_setup
        service = PostgreSQLService()
        
        records = [{
            'unique_id': 'b1', 'province_name': 'P1', 
            'district_name': 'D1', 'sector_name': 'S1', 
            'village_name': 'V1'
        }]
        
        # Configure returns:
        # Prequel: SELECT 1 (x2)
        # 1. CREATE TABLE
        # 2. UPSERT
        mock_result_dummy = MagicMock()
        mock_result_create = MagicMock()
        
        mock_result_upsert = MagicMock()
        mock_result_upsert.fetchone.return_value = [True]
        
        mock_connection.execute.side_effect = [
            mock_result_dummy,
            mock_result_dummy,
            mock_result_create,
            mock_result_upsert
        ]
        
        success, result = service.save_boundaries_data(records)
        
        assert success is True
        assert result['inserted'] == 1

    def test_save_error_handling(self, mock_engine_setup):
        _, mock_engine, _ = mock_engine_setup
        service = PostgreSQLService()
        
        # Simmsulate engine begin failing
        mock_engine.begin.side_effect = Exception("DB Connection Error")
        
        success, message = service.save_raw_data([{'a': 1}])
        assert success is False
        assert "Database save failed" in message
