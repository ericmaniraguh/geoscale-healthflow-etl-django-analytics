import pytest
from django.contrib.auth import get_user_model
from django.test import Client

@pytest.fixture
def client():
    return Client()

@pytest.fixture
def user_data():
    return {
        "username": "testuser",
        "email": "test@example.com",
        "password": "StrongPassword!123",
        "confirm_password": "StrongPassword!123"
    }

@pytest.fixture
def create_user(user_data):
    User = get_user_model()
    def _create_user(**kwargs):
        data = user_data.copy()
        data.update(kwargs)
        # remove confirm_password for create_user
        if 'confirm_password' in data:
            del data['confirm_password']
        return User.objects.create_user(**data)
    return _create_user

@pytest.fixture
def authenticated_client(client, create_user):
    user = create_user()
    client.force_login(user)
    return client, user

@pytest.fixture
def admin_client(client, create_user):
    user = create_user(is_staff=True, is_superuser=True)
    client.force_login(user)
    return client, user

@pytest.fixture
def create_analytics_tables(db):
    from django.db import connection
    with connection.cursor() as cursor:
        # Drop tables to ensure fresh schema matching our definitions
        cursor.execute("DROP TABLE IF EXISTS hc_data_yearly_statist_bugesera_kamabuye;")
        cursor.execute("DROP TABLE IF EXISTS hc_api_east_bugesera;")
        cursor.execute("DROP TABLE IF EXISTS hc_raw_bugesera_kamabuye;")
        cursor.execute("DROP TABLE IF EXISTS weather_juru_prec_and_juru_temp_no_district;")
        cursor.execute("DROP TABLE IF EXISTS hc_data_monthly_positivity_bugesera_kamabuye;")

        cursor.execute("CREATE TABLE hc_data_yearly_statist_bugesera_kamabuye (total_tests INT, positive_cases INT, negative_cases INT, positivity_rate FLOAT, year INT, filter_district VARCHAR, filter_sector VARCHAR);")
        cursor.execute("CREATE TABLE hc_api_east_bugesera (population INT, api FLOAT, province VARCHAR, district VARCHAR, sector VARCHAR);")
        cursor.execute("CREATE TABLE hc_raw_bugesera_kamabuye (gender VARCHAR, test_result VARCHAR, is_positive BOOLEAN, year INT, district VARCHAR, sector VARCHAR, village VARCHAR);")
        cursor.execute("CREATE TABLE weather_juru_prec_and_juru_temp_no_district (year INT, month INT, monthly_precipitation FLOAT, monthly_temperature FLOAT, district VARCHAR, prec_station VARCHAR, temp_station VARCHAR, metadata VARCHAR);")
        cursor.execute("CREATE TABLE hc_data_monthly_positivity_bugesera_kamabuye (month_name VARCHAR, positivity_rate FLOAT, year INT, month INT, filter_district VARCHAR, filter_sector VARCHAR);")
