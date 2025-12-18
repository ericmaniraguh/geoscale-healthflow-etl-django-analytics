"""
Django settings for GeoScalable Malaria Project - CORRECTED FOR OTP SYSTEM
"""

from pathlib import Path
import os
from decouple import config
from mongoengine import connect

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# ================================
# DJANGO CORE SETTINGS
# ================================

SECRET_KEY = config('SECRET_KEY')
DEBUG = config('DEBUG', default=False, cast=bool)
ALLOWED_HOSTS = ['*'] if DEBUG else ['your-domain.com']

# ================================
# APPLICATION DEFINITION
# ================================

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'mongoengine', 
    'django_extensions',

    # Third party apps - REMOVED CONFLICTING OTP APPS
    'allauth',
    'allauth.account',
    'allauth.socialaccount',
    'allauth.socialaccount.providers.google',
    # REMOVED: 'django_otp' and related plugins - using custom OTP implementation

    # All apps in the project
    'app.accounts.apps.AccountsConfig',
    'app.upload_app.apps.UploadAppConfig',
    'app.etl_app.apps.EtlAppConfig',
    'app.geospatial_merger.apps.GeospatialMergerConfig',
    'app.analytics_dashboard.apps.AnalyticsDashboardConfig',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',  # ADDED: WhiteNoise for static files
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    # REMOVED: 'django_otp.middleware.OTPMiddleware' - not needed for custom OTP
    "allauth.account.middleware.AccountMiddleware", 
]

# Site ID for allauth
SITE_ID = 1 

# Authentication backends
AUTHENTICATION_BACKENDS = [
    'django.contrib.auth.backends.ModelBackend',
    'allauth.account.auth_backends.AuthenticationBackend',
]

# ================================
# ALLAUTH SETTINGS (UPDATED FOR COMPATIBILITY)
# ================================

# IMPORTANT: Disable allauth email verification to use our custom system
ACCOUNT_EMAIL_VERIFICATION = "none"  # CHANGED: Use our custom verification instead
ACCOUNT_LOGOUT_REDIRECT_URL = '/accounts/login/'
ACCOUNT_EMAIL_REQUIRED = True
# ================================
# ALLAUTH SETTINGS (UPDATED - NON-DEPRECATED)
# ================================

# Login methods (replaces ACCOUNT_AUTHENTICATION_METHOD)
ACCOUNT_LOGIN_METHODS = {'username', 'email'}

# Signup fields (replaces ACCOUNT_EMAIL_REQUIRED and ACCOUNT_USERNAME_REQUIRED)
ACCOUNT_SIGNUP_FIELDS = ['email*', 'username*', 'password1*', 'password2*']

# Rate limits (replaces ACCOUNT_LOGIN_ATTEMPTS_LIMIT/TIMEOUT)
ACCOUNT_RATE_LIMITS = {
    'login_failed': '5/5m',  # 5 attempts per 5 minutes
}



# Disable allauth signup to use our custom registration
ACCOUNT_SIGNUP_ENABLED = False  # ADDED: Force users to use our custom signup

ROOT_URLCONF = 'app.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'app.wsgi.application'

# ================================
# DATABASE CONFIGURATION (UNCHANGED)
# ================================
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': config('STAGING_DB_NAME'),
        'USER': config('STAGING_DB_USER'),
        'PASSWORD': config('STAGING_DB_PASSWORD'),
        'HOST': config('STAGING_DB_HOST'),
        'PORT': config('STAGING_DB_PORT', cast=int),
        'OPTIONS': {
            'connect_timeout': 10,
        }
    }
}

# ================================
# EMAIL CONFIGURATION (SIMPLE VERSION)
# ================================

# SMTP Configuration
EMAIL_HOST = 'smtp.gmail.com'
EMAIL_PORT = 587
EMAIL_USE_TLS = True
EMAIL_HOST_USER = config('EMAIL_HOST_USER', default='')
EMAIL_HOST_PASSWORD = config('EMAIL_HOST_PASSWORD', default='')

# For testing only
EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'

# Default from email
DEFAULT_FROM_EMAIL = f"GeoScale Malaria Platform <{config('EMAIL_HOST_USER', default='noreply@example.com')}>"
SERVER_EMAIL = config('EMAIL_HOST_USER', default='')

# Site URL for email links
# SITE_URL = config('SITE_URL', default='http://localhost:8000')

# ================================
# CUSTOM OTP SETTINGS
# ================================

OTP_EXPIRY_SECONDS = 300
OTP_RESEND_COOLDOWN = 30
OTP_MAX_ATTEMPTS = 3
ACCOUNT_LOCKOUT_DURATION = 900
# ================================
# SOCIAL ACCOUNT SETTINGS (CORRECTED)
# ================================

# Google OAuth credentials
# Social account settings (keep your existing Google OAuth settings)
SOCIALACCOUNT_PROVIDERS = {
    'google': {
        'APP': {
            'client_id': config('GOOGLE_OAUTH2_KEY', default=''),
            'secret': config('GOOGLE_OAUTH2_SECRET', default=''),
            'key': ''
        },
        'SCOPE': ['profile', 'email'],
        'AUTH_PARAMS': {'access_type': 'online'},
        'OAUTH_PKCE_ENABLED': True,
    }
}

# ================================
# LOGGING CONFIGURATION (IMPROVED)
# ================================

# Create logs directory if it doesn't exist
LOG_DIR = BASE_DIR / 'logs'
os.makedirs(LOG_DIR, exist_ok=True)

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '[{asctime}] {levelname} {module}: {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname}: {message}',
            'style': '{',
        },
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
        'file': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': LOG_DIR / 'django.log',
            'formatter': 'verbose',
        },
        'accounts_file': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': LOG_DIR / 'accounts.log',
            'formatter': 'verbose',
        },
    },
    'root': {
        'handlers': ['console'],
        'level': 'INFO',
    },
    'loggers': {
        'django': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
        'accounts': {
            'handlers': ['console', 'accounts_file'],
            'level': 'DEBUG' if DEBUG else 'INFO',
            'propagate': False,
        },
        'app': {
            'handlers': ['console', 'file'],
            'level': 'DEBUG' if DEBUG else 'INFO',
            'propagate': False,
        },
    },
}

# ==================================
#  CACHING CONFIGURATION (ADDED)
#===================================

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'unique-snowflake',
        'TIMEOUT': 3600,  # 1 hour
        'OPTIONS': {
            'MAX_ENTRIES': 1000
        }
    }
}

# ================================
# MONGODB CONFIGURATION (UNCHANGED - YOUR EXISTING CONFIG)
# ================================

# MONGO_URI = config('MONGO_URI', default='mongodb://localhost:27017/')

MONGO_URI = config(
    'MONGO_URI',
    default='mongodb+srv://healthcenter_admin:admin@healthcentercluster.4v2d6.mongodb.net/'
)

MONGO_DB = config('MONGO_DB', default='health_center_data')
MONGO_COLLECTION = config('MONGO_COLLECTION', default='lab_data')
MONGO_HEALTHCENTER_DB = config('MONGO_HEALTHCENTER_DB', default=MONGO_DB)
MONGO_HEALTHCENTER_COLLECTION = config('MONGO_HEALTHCENTER_COLLECTION', default=MONGO_COLLECTION)
MONGO_HEALTHCENTER_METADATA_COLLECTION = f"{MONGO_HEALTHCENTER_COLLECTION}_metadata"

MONGO_HMIS_DB = config('MONGO_HMIS_DB', default='hmis_data')
MONGO_HMIS_COLLECTION = config('MONGO_HMIS_COLLECTION', default='hmis_records')
MONGO_HMIS_METADATA_COLLECTION = f"{MONGO_HMIS_COLLECTION}_metadata"

MONGO_WEATHER_DB = config('MONGO_WEATHER_DB', default='weather_data')
MONGO_TEMP_COLLECTION = config('MONGO_TEMP_COLLECTION', default='temperature')
MONGO_PREC_COLLECTION = config('MONGO_PREC_COLLECTION', default='precipitation')
MONGO_WEATHER_TEMP_METADATA_COLLECTION = f"{MONGO_TEMP_COLLECTION}_metadata"
MONGO_WEATHER_PREC_METADATA_COLLECTION = f"{MONGO_PREC_COLLECTION}_metadata"

# MONGO_SHAPEFILE_URI = config('MONGO_SHAPEFILE_URI', default=MONGO_URI)
MONGO_SHAPEFILE_URI = MONGO_URI # Force use of main Cluster URI to fix localhost issue
MONGO_SHAPEFILE_DB = config('MONGO_SHAPEFILE_DB', default='geospatial_wgs84_boundaries_db') # UPDATED to match processor expectations
MONGO_SHAPEFILE_COLLECTION = config('MONGO_SHAPEFILE_COLLECTION', default='boundaries_slope_wgs84') # UPDATED to match processor expectations

MONGO_SLOPE_DB = config('MONGO_DB_NAME', default='slope_raster_database')
MONGO_SLOPE_COLLECTION = config('MONGO_COLLECTION_NAME', default='slope_uploads')

MONGO_DB_GEOJSON_NAME = config("MONGO_DB_GEOJSON", default='slope_raster_GeoTiff_db')
MONGO_COLLECTION_GEOJSON_NAME = config("MONGO_COLLECTION_GEOJSON", default='slope_Geotiff_uploads')

def initialize_mongodb():
    """Initialize MongoDB connection safely"""
    try:
        connect(db=MONGO_DB, host=MONGO_URI, alias='default')
        return True
    except Exception as e:
        print(f"MongoEngine connection failed: {e}")
        return False

MONGODB_CONNECTED = initialize_mongodb()

# ================================
# OTHER SETTINGS (UNCHANGED)
# ================================

REST_FRAMEWORK = {
    'DEFAULT_RENDERER_CLASSES': [
        'rest_framework.renderers.JSONRenderer',
        'rest_framework.renderers.BrowsableAPIRenderer',
    ],
    'DEFAULT_PARSER_CLASSES': [
        'rest_framework.parsers.JSONParser',
        'rest_framework.parsers.MultiPartParser',
        'rest_framework.parsers.FileUploadParser',
    ],
    'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.AllowAny',
    ],
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    'PAGE_SIZE': 20
}

AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'Africa/Kigali'
USE_I18N = True
USE_L10N = True
USE_TZ = True

AUTH_USER_MODEL = 'accounts.CustomUser'
LOGIN_REDIRECT_URL = '/upload/dashboard/'
LOGOUT_REDIRECT_URL = '/accounts/login/'

STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles')
STATICFILES_DIRS = [BASE_DIR / "static"]

# Enable WhiteNoise compression and caching
STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'

MEDIA_URL = '/media/'
MEDIA_ROOT = os.path.join(BASE_DIR, 'media')

# Security settings for production
if not DEBUG:
    SECURE_SSL_REDIRECT = True
    SECURE_HSTS_SECONDS = 31536000
    SECURE_HSTS_INCLUDE_SUBDOMAINS = True
    SECURE_HSTS_PRELOAD = True
    SECURE_CONTENT_TYPE_NOSNIFF = True
    SECURE_BROWSER_XSS_FILTER = True
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    X_FRAME_OPTIONS = 'DENY'
    
    # Use SSL for email in production
    EMAIL_USE_SSL = True
    EMAIL_PORT = 465
    EMAIL_USE_TLS = False

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# File upload settings
FILE_UPLOAD_MAX_MEMORY_SIZE = 10 * 1024 * 1024
DATA_UPLOAD_MAX_MEMORY_SIZE = 10 * 1024 * 1024

# Geospatial settings
GDAL_LIBRARY_PATH = config('GDAL_LIBRARY_PATH', default='')
GEOS_LIBRARY_PATH = config('GEOS_LIBRARY_PATH', default='')


print("Database Configuration Loaded:")
print(f"   PostgreSQL: {DATABASES['default']['NAME']}@{DATABASES['default']['HOST']}")
print(f"   MongoDB: {'Connected' if MONGODB_CONNECTED else 'Connection Failed'}")
print(f"   Email Backend: {EMAIL_BACKEND}")
print(f"   Custom OTP System: Enabled")


