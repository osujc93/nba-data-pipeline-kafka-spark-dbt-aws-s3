# File: superset_config.py
# /app/config/superset_config.py

import logging
import os

from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

pg_user: str = os.getenv("POSTGRES_USER", "")
pg_password: str = os.getenv("POSTGRES_PASSWORD", "")
pg_host: str = os.getenv("POSTGRES_HOST", "postgres")
pg_port: str = os.getenv("POSTGRES_PORT", "5432")

pg_db_superset: str = os.getenv("POSTGRES_SUPERSET_DB", "superset")
pg_db_trino: str = os.getenv("POSTGRES_TRINO_DB", "iceberg")

SQLALCHEMY_DATABASE_URI: str = (
    f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db_superset}"
)

TRINO_PROTOCOL: str = "http"
TRINO_DATABASE_URI: str = (
    f"trino://{pg_user}:{pg_password}@trino:8085/{pg_db_trino}?protocol={TRINO_PROTOCOL}"
)

SUPERSET_WEBSERVER_PORT: int = 8098
SECRET_KEY: str = os.getenv("SUPERSET_SECRET_KEY", "change_this_in_production")

CACHE_CONFIG = {
    "CACHE_TYPE": "redis",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": "redis",
    "CACHE_REDIS_PORT": 6379,
    "CACHE_REDIS_DB": 1,
    "CACHE_REDIS_URL": "redis://redis:6379/1",
}

RATELIMIT_STORAGE_URI: str = "redis://redis:6379/2"

def setup_rate_limiting(app):
    """
    Initialize Flask-Limiter for rate limiting.
    """
    limiter = Limiter(
        get_remote_address,
        storage_uri=RATELIMIT_STORAGE_URI,
        strategy="fixed-window",
    )
    limiter.init_app(app)

# --------------------------------------------------------------------------------
# [ADDED for Debugging]
# Set everything to DEBUG, echo SQL
DEBUG = True
LOG_LEVEL = "DEBUG"
SQLALCHEMY_ECHO = True
# --------------------------------------------------------------------------------

ENABLE_TIME_ROTATE = True
FILENAME = "/var/log/superset/superset.log"
ROLLOVER = "midnight"
BACKUP_COUNT = 30

if not os.path.exists("/var/log/superset"):
    os.makedirs("/var/log/superset", exist_ok=True)

if not os.path.exists(FILENAME):
    open(FILENAME, "a").close()

logging.basicConfig(
    filename=FILENAME,
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s",
)
