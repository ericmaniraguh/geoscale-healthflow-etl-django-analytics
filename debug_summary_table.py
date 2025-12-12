import os
import django
from django.db import connection

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'app.settings')
django.setup()

def debug_summary_columns():
    table_name = 'hc_data_yearly_statist_bugesera_kamabuye'
    print(f"--- Debugging Table: {table_name} ---")
    
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        print("\n--- Column Names ---")
        columns = [col[0] for col in cursor.description]
        print(columns)
        
        if 'village' in columns:
            print("Village column EXISTS.")
        else:
            print("Village column does NOT exist.")

if __name__ == "__main__":
    debug_summary_columns()
