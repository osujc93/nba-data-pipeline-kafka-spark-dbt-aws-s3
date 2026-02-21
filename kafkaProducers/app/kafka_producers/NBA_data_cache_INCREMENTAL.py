#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import hashlib
import threading
import logging
from typing import Any, Dict, Optional

import psycopg2
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NBADataCacheIncremental:
    """
    A variant of the original NBADataCache that also updates a Postgres table
    with the last retrieval date for better traceability.

    For demonstration, we store data the same way (in-memory),
    but after each set() we also can do optional logic to update a table
    storing "last_fetched" or similar. This is just an example approach.
    """

    _cache: Dict[str, Dict[str, Any]] = {}
    _lock: threading.Lock = threading.Lock()

    DEFAULT_TTL: Optional[int] = None  # None => infinite

    @classmethod
    def _make_key(cls, url: str) -> str:
        return hashlib.sha256(url.encode('utf-8')).hexdigest()

    @classmethod
    def get(cls, url: str) -> Optional[Any]:
        with cls._lock:
            key = cls._make_key(url)
            entry = cls._cache.get(key)
            if not entry:
                return None
            expires_at = entry.get('expires_at')
            if expires_at is not None and time.time() > expires_at:
                cls._cache.pop(key, None)
                return None
            return entry['data']

    @classmethod
    def set(cls, url: str, data: Any, ttl: Optional[int] = None) -> None:
        with cls._lock:
            if ttl is None:
                ttl = cls.DEFAULT_TTL
            expires_at = time.time() + ttl if ttl else None
            key = cls._make_key(url)
            cls._cache[key] = {
                'data': data,
                'expires_at': expires_at
            }

        logger.info(f"[CACHE-Incremental] Cached data for {url} (ttl={ttl}).")
        # Optionally, we could store a row to Postgres indicating we just fetched from that URL
        cls._update_postgres_last_fetched(url)

    @classmethod
    def _update_postgres_last_fetched(cls, url: str) -> None:
        """
        Example method that simply writes a row to a separate 'cache_fetch_log' table in Postgres
        if you want a record that you fetched data for a certain URL at a certain time.

        This is entirely optional, just demonstrating how you'd tie cache logic to DB.
        """
        try:
            conn = psycopg2.connect(
                dbname="nelomlb",
                user="nelomlb",
                password="Pacmanbrooklyn19",
                host="postgres",
                port=5432
            )
            conn.autocommit = True
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS cache_fetch_log (
                    id SERIAL PRIMARY KEY,
                    fetch_url TEXT NOT NULL,
                    fetched_at TIMESTAMP NOT NULL
                );
            """)

            cur.execute("""
                INSERT INTO cache_fetch_log (fetch_url, fetched_at)
                VALUES (%s, %s);
            """, (url, datetime.now()))

        except Exception as e:
            logger.error(f"[CACHE-Incremental] Error updating cache_fetch_log: {e}")
        finally:
            if 'conn' in locals():
                conn.close()
