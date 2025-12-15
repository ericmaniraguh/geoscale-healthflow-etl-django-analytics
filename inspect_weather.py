import os
import django
from django.db import connection

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'app.settings')
django.setup()

def inspect_weather_table():
    table_name = "weather_juru_prec_and_juru_temp_no_district"
    try:
        with connection.cursor() as cursor:
            # Check if table exists
            cursor.execute("SELECT to_regclass(%s)", [table_name])
            if not cursor.fetchone()[0]:
                print(f"Table {table_name} does not exist.")
                return

            # Get columns
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
            col_names = [desc[0] for desc in cursor.description]
            print(f"Columns: {col_names}")

            # Check distinct districts
            if 'district' in col_names:
                cursor.execute(f"SELECT DISTINCT district FROM {table_name}")
                districts = cursor.fetchall()
                print(f"Distinct Districts: {[d[0] for d in districts]}")
            else:
                print("Column 'district' not found.")
                
            # Check distinct stations
            if 'prec_station' in col_names:
                cursor.execute(f"SELECT DISTINCT prec_station FROM {table_name}")
                stations = cursor.fetchall()
                print(f"Precipitation Stations: {[s[0] for s in stations]}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_weather_table()
