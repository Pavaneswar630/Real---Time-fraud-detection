"""
Django Settings for fraud_service
=================================
Configured for PostgreSQL 15, Redis Alpine Feature Store, and High-Throughput REST Serving.
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "fraud-pipeline-super-secret-production-key-999")

DEBUG = os.getenv("DJANGO_DEBUG", "True").lower() in ("true", "1")

ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    # 'django.contrib.admin',  <- Remove or comment out
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    # 'django.contrib.messages', <- Remove or comment out
    'django.contrib.staticfiles',
    'rest_framework',
    'scoring',
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "fraud_service.urls"

TEMPLATES = []

WSGI_APPLICATION = "fraud_service.wsgi.application"
ASGI_APPLICATION = "fraud_service.asgi.application"

# -------------------------------------------------------------------------
# Database Configuration: PostgreSQL 15 with SQLite fallback for local test
# -------------------------------------------------------------------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "fraud_db")
DB_USER = os.getenv("DB_USER", "fraud_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "fraud_password")
import socket

def is_postgres_available(host, port):
    try:
        with socket.create_connection((host, int(port)), timeout=0.5):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

USE_SQLITE = os.getenv("USE_SQLITE", "false").lower() in ("true", "1")

if USE_SQLITE or not is_postgres_available(DB_HOST, DB_PORT):

    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": BASE_DIR / "db.sqlite3",
        }
    }
else:

    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": DB_NAME,
            "USER": DB_USER,
            "PASSWORD": DB_PASSWORD,
            "HOST": DB_HOST,
            "PORT": DB_PORT,
            "CONN_MAX_AGE": 60,  # Persistent DB connection pooling for low latency
        }
    }

# -------------------------------------------------------------------------
# Redis Feature Store Configuration
# -------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# -------------------------------------------------------------------------
# Django REST Framework Settings
# -------------------------------------------------------------------------
REST_FRAMEWORK = {
    "DEFAULT_RENDERER_CLASSES": [
        "rest_framework.renderers.JSONRenderer",
    ],
    "DEFAULT_PARSER_CLASSES": [
        "rest_framework.parsers.JSONParser",
    ],
    "DEFAULT_THROTTLE_CLASSES": [
        "scoring.throttles.CapacityPlannedRateThrottle",
    ],

    "DEFAULT_THROTTLE_RATES": {
        "anon": "72000/min",
    },
    "UNAUTHENTICATED_USER": None,
}


LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
