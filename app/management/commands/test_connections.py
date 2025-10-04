# app/management/commands/test_connections.py

from django.core.management.base import BaseCommand
from django.db import connection
from django.db.utils import OperationalError
from django.conf import settings
from pymongo import MongoClient
import psycopg2

class Command(BaseCommand):
    help = 'Test database connections for PostgreSQL and MongoDB'

    def add_arguments(self, parser):
        parser.add_argument(
            '--postgresql',
            action='store_true',
            help='Test only PostgreSQL connection',
        )
        parser.add_argument(
            '--mongodb',
            action='store_true',
            help='Test only MongoDB connection',
        )

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.HTTP_INFO('🔍 Testing Database Connections...\n')
        )

        # Test PostgreSQL if requested or by default
        if options['postgresql'] or not any([options['postgresql'], options['mongodb']]):
            self.test_postgresql()

        # Test MongoDB if requested or by default  
        if options['mongodb'] or not any([options['postgresql'], options['mongodb']]):
            self.test_mongodb()

    def test_postgresql(self):
        """Test PostgreSQL connection"""
        self.stdout.write('📊 Testing PostgreSQL Connection...')
        
        try:
            # Method 1: Using Django ORM
            with connection.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()
                
            self.stdout.write(
                self.style.SUCCESS(f'✅ Django PostgreSQL Connection: SUCCESS')
            )
            self.stdout.write(f'   Database: {connection.settings_dict["NAME"]}')
            self.stdout.write(f'   Host: {connection.settings_dict["HOST"]}:{connection.settings_dict["PORT"]}')
            self.stdout.write(f'   Version: {version[0][:50]}...\n')
            
        except OperationalError as e:
            self.stdout.write(
                self.style.ERROR(f'❌ Django PostgreSQL Connection: FAILED')
            )
            self.stdout.write(f'   Error: {e}\n')
            
            # Try direct connection for more details
            self.test_postgresql_direct()
            return

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'❌ Django PostgreSQL Connection: ERROR')
            )
            self.stdout.write(f'   Error: {e}\n')

    def test_postgresql_direct(self):
        """Test PostgreSQL with direct connection"""
        self.stdout.write('🔧 Testing Direct PostgreSQL Connection...')
        
        try:
            db_config = settings.DATABASES['default']
            
            conn = psycopg2.connect(
                host=db_config['HOST'],
                port=db_config['PORT'],
                database=db_config['NAME'],
                user=db_config['USER'],
                password=db_config['PASSWORD']
            )
            
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            
            # Test if database exists
            cursor.execute(
                "SELECT datname FROM pg_database WHERE datname = %s;", 
                (db_config['NAME'],)
            )
            db_exists = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            self.stdout.write(
                self.style.SUCCESS(f'✅ Direct PostgreSQL Connection: SUCCESS')
            )
            self.stdout.write(f'   Database exists: {"Yes" if db_exists else "No"}')
            self.stdout.write(f'   Version: {version[0][:50]}...\n')
            
        except psycopg2.Error as e:
            self.stdout.write(
                self.style.ERROR(f'❌ Direct PostgreSQL Connection: FAILED')
            )
            self.stdout.write(f'   Error Code: {e.pgcode}')
            self.stdout.write(f'   Error: {e}\n')
            
            # Provide troubleshooting tips
            self.stdout.write(self.style.WARNING('💡 Troubleshooting Tips:'))
            self.stdout.write('   1. Check if PostgreSQL service is running')
            self.stdout.write('   2. Verify database exists: data_pipeline_hc_staging_db')
            self.stdout.write('   3. Check user permissions for postgres user')
            self.stdout.write('   4. Verify connection settings in .env file\n')
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'❌ Direct PostgreSQL Connection: ERROR')
            )
            self.stdout.write(f'   Error: {e}\n')

    def test_mongodb(self):
        """Test MongoDB connection"""
        self.stdout.write('🍃 Testing MongoDB Connection...')
        
        try:
            # Test MongoEngine connection
            from mongoengine import connection as me_connection
            
            # Get default connection
            db = me_connection.get_db()
            
            self.stdout.write(
                self.style.SUCCESS(f'✅ MongoEngine Connection: SUCCESS')
            )
            self.stdout.write(f'   Database: {db.name}')
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'❌ MongoEngine Connection: FAILED')
            )
            self.stdout.write(f'   Error: {e}')

        try:
            # Test PyMongo connection
            mongo_uri = getattr(settings, 'MONGO_URI', 'mongodb://localhost:27017/')
            client = MongoClient(mongo_uri)
            
            # Test connection
            client.admin.command('ping')
            
            # List databases
            db_list = client.list_database_names()
            
            self.stdout.write(
                self.style.SUCCESS(f'✅ PyMongo Connection: SUCCESS')
            )
            self.stdout.write(f'   URI: {mongo_uri}')
            self.stdout.write(f'   Available DBs: {", ".join(db_list[:5])}{"..." if len(db_list) > 5 else ""}\n')
            
            client.close()
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'❌ PyMongo Connection: FAILED')
            )
            self.stdout.write(f'   Error: {e}')
            
            # Provide troubleshooting tips
            self.stdout.write(self.style.WARNING('💡 Troubleshooting Tips:'))
            self.stdout.write('   1. Check if MongoDB service is running')
            self.stdout.write('   2. Verify MONGO_URI in .env file')
            self.stdout.write('   3. Check MongoDB authentication settings\n')

    def test_database_operations(self):
        """Test basic database operations"""
        self.stdout.write('🧪 Testing Database Operations...')
        
        try:
            # Test PostgreSQL table creation
            with connection.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS test_connection (
                        id SERIAL PRIMARY KEY,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                cursor.execute("INSERT INTO test_connection DEFAULT VALUES;")
                cursor.execute("SELECT COUNT(*) FROM test_connection;")
                count = cursor.fetchone()[0]
                
                cursor.execute("DROP TABLE test_connection;")
                
            self.stdout.write(
                self.style.SUCCESS(f'✅ PostgreSQL Operations: SUCCESS (inserted {count} rows)')
            )
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'❌ PostgreSQL Operations: FAILED - {e}')
            )