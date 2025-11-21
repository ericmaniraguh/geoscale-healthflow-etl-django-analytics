"""
Fix migration inconsistency by manually marking accounts.0003 as applied
"""
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'app.settings')
django.setup()

from django.db import connection

def fix_migration_history():
    """Manually insert the missing migration record"""
    with connection.cursor() as cursor:
        # Check if the migration record exists
        cursor.execute("""
            SELECT id FROM django_migrations
            WHERE app = 'accounts' AND name = '0003_alter_customuser_options_and_more'
        """)

        result = cursor.fetchone()

        if result:
            print("[INFO] Migration accounts.0003 is already recorded")
        else:
            print("[INFO] Inserting migration record for accounts.0003...")
            cursor.execute("""
                INSERT INTO django_migrations (app, name, applied)
                VALUES ('accounts', '0003_alter_customuser_options_and_more', NOW())
            """)
            print("[SUCCESS] Migration record inserted successfully!")
            print("Now you can run: python manage.py migrate")

if __name__ == "__main__":
    try:
        fix_migration_history()
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        print("\nAlternative: You can manually run this SQL in pgAdmin:")
        print("""
        INSERT INTO django_migrations (app, name, applied)
        VALUES ('accounts', '0003_alter_customuser_options_and_more', NOW());
        """)
