#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBADataCache - A simple in-memory cache that stores NBA data indefinitely by default
unless a TTL is explicitly provided. If DEFAULT_TTL is None, data never expires.
"""

import time
import hashlib
import threading
import logging
from typing import Any, Dict, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger: logging.Logger = logging.getLogger(__name__)


class NBADataCache:
    """
    A simple in-memory cache with an optional TTL (time-to-live).
    If DEFAULT_TTL is None, data will never expire unless overwritten.

    Cache key is derived from the request URL (which includes all relevant params).
    """

    _cache: Dict[str, Dict[str, Any]] = {}
    _lock: threading.Lock = threading.Lock()

    # CHANGED: Default TTL is now None => infinite
    DEFAULT_TTL: Optional[int] = None

    @classmethod
    def _make_key(cls, url: str) -> str:
        """
        Create a hash key from the provided URL.
        """
        return hashlib.sha256(url.encode('utf-8')).hexdigest()

    @classmethod
    def get(cls, url: str) -> Optional[Any]:
        """
        Retrieve cached data for the given URL if it exists and, if TTL applies, is not expired.
        If TTL is None, the data never expires.
        """
        with cls._lock:
            key: str = cls._make_key(url)
            entry: Optional[Dict[str, Any]] = cls._cache.get(key)

            if not entry:
                return None

            expires_at: Optional[float] = entry.get('expires_at')

            # If expires_at is None, it never expires
            if expires_at is not None and time.time() > expires_at:
                cls._cache.pop(key, None)
                return None

            if expires_at is not None:
                ttl_remaining = expires_at - time.time()
                logger.info(
                    f"[CACHE] Data for URL {url} found in cache; "
                    f"TTL {ttl_remaining:.2f}s remaining."
                )
            else:
                logger.info(
                    f"[CACHE] Data for URL {url} found in cache; no expiration set (infinite)."
                )

            return entry['data']

    @classmethod
    def set(cls, url: str, data: Any, ttl: Optional[int] = None) -> None:
        """
        Store data in the cache for the given URL.

        :param url: The unique URL or string used to build the cache key.
        :param data: The data to cache (often a dict or list).
        :param ttl: Time-to-live in seconds. If None, falls back to DEFAULT_TTL.
                    If DEFAULT_TTL is also None, data never expires.
        """
        with cls._lock:
            # Determine the effective TTL
            if ttl is None:
                ttl = cls.DEFAULT_TTL

            if ttl is not None:
                expires_at: Optional[float] = time.time() + ttl
            else:
                expires_at = None

            key: str = cls._make_key(url)
            existing_entry: Optional[Dict[str, Any]] = cls._cache.get(key)

            # If we already have data in the cache, compare it to the new data
            if existing_entry is not None:
                # If the existing data is identical, skip overwriting
                if existing_entry['data'] == data:
                    logger.info(
                        f"[CACHE] Data for URL {url} is unchanged; "
                        "skipping cache update."
                    )
                    return

            cls._cache[key] = {
                'data': data,
                'expires_at': expires_at
            }

            if expires_at is not None:
                ttl_seconds = int(expires_at - time.time())
                logger.info(
                    f"[CACHE] Data for URL {url} stored in cache; expires in {ttl_seconds}s."
                )
            else:
                logger.info(
                    f"[CACHE] Data for URL {url} stored in cache; no expiration (infinite)."
                )
