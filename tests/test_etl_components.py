import pytest
from app.etl_app.forms import (
    RwandaBoundariesForm, 
    MalariaAPIForm, 
    HealthCenterForm, 
    WeatherPrecTempForm, 
    SlopeGeoJSONForm
)
from app.etl_app.schemas.table_schemas import TableSchemas
from app.etl_app.utils import constants

class TestETLForms:
    def test_rwanda_boundaries_form_valid(self):
        data = {'district': 'Bugesera', 'sector': 'Kamabuye'}
        form = RwandaBoundariesForm(data=data)
        assert form.is_valid()
        
    def test_rwanda_boundaries_form_invalid(self):
        data = {'district': ''} # Missing sector
        form = RwandaBoundariesForm(data=data)
        assert not form.is_valid()
        assert 'sector' in form.errors

    def test_malaria_api_form_valid(self):
        data = {'province': 'East', 'district': 'Bugesera', 'years': '2023'}
        form = MalariaAPIForm(data=data)
        assert form.is_valid()

    def test_health_center_form_valid(self):
        data = {'years': '2023', 'district': 'Bugesera', 'sector': 'Kamabuye'}
        form = HealthCenterForm(data=data)
        assert form.is_valid()

    def test_weather_prec_temp_form_valid(self):
        data = {'years': '2023'}
        form = WeatherPrecTempForm(data=data)
        assert form.is_valid()

    def test_slope_geojson_form_valid(self):
        data = {
            'extraction_type': 'coordinates', 
            'district': 'Kigali', 
            'sector': 'Nyarugenge',
            'min_lon': 29.85,
            'min_lat': -1.06,
            'max_lon': 29.87,
            'max_lat': -1.04
        }
        form = SlopeGeoJSONForm(data=data)
        assert form.is_valid()

class TestETLSchemas:
    def test_get_raw_data_schema(self):
        sql = TableSchemas.get_raw_data_schema("test_table")
        assert "CREATE TABLE test_table" in sql
        assert "unique_id VARCHAR(36)" in sql

    def test_get_raw_data_indexes(self):
        sql = TableSchemas.get_raw_data_indexes("test_table")
        assert "CREATE INDEX idx_test_table_unique_id" in sql

    def test_get_yearly_stats_schema(self):
        sql = TableSchemas.get_yearly_stats_schema("test_stats")
        assert "CREATE TABLE test_stats" in sql
        assert "positivity_rate DECIMAL(5,2)" in sql

    def test_get_gender_pos_schema(self):
        sql = TableSchemas.get_gender_pos_schema("test_gender")
        assert "CREATE TABLE test_gender" in sql

    def test_get_village_pos_schema(self):
        sql = TableSchemas.get_village_pos_schema("test_village")
        assert "CREATE TABLE test_village" in sql

    def test_get_monthly_pos_schema(self):
        sql = TableSchemas.get_monthly_pos_schema("test_monthly")
        assert "CREATE TABLE test_monthly" in sql

    def test_get_summary_schema(self):
        sql = TableSchemas.get_summary_schema("test_summary")
        assert "CREATE TABLE test_summary" in sql

class TestETLConstants:
    def test_constants_structure(self):
        assert 'REPLACE' in constants.UPDATE_MODES
        assert constants.DEFAULT_AGE == 30
        assert 'MALE_VARIANTS' in constants.GENDER_MAPPINGS
        assert 1 in constants.MONTH_NAMES
        assert 'january' in constants.MONTH_ABBREVIATIONS
        assert len(constants.ETL_FEATURES) > 0
