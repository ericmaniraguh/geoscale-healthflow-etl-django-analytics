import os
import django
from django.db import connection

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'app.settings')
django.setup()

def debug_village_query():
    print("--- Debugging Village Query ---")
    
    # Simulate filters
    years = [2021, 2022, 2023]
    district = None
    sector = None
    
    query = """
        SELECT 
            village, 
            COUNT(*) as total_tests,
            SUM(CASE WHEN test_result = 'Positive' OR is_positive = true THEN 1 ELSE 0 END) as positive_cases
        FROM hc_raw_bugesera_kamabuye 
        WHERE 1=1
    """
    params = []
    
    if years:
        query += " AND year IN %s"
        params.append(tuple(years))
        
    query += " GROUP BY village HAVING COUNT(*) > 0 ORDER BY (SUM(CASE WHEN test_result = 'Positive' OR is_positive = true THEN 1 ELSE 0 END)::float / COUNT(*)) DESC LIMIT 50"
    
    print(f"Query: {query}")
    print(f"Params: {params}")
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            print(f"Rows returned: {len(rows)}")
            if len(rows) > 0:
                print("First row:", rows[0])
    except Exception as e:
        print(f"SQL Error: {e}")

if __name__ == "__main__":
    debug_village_query()
