"""
Result caching for discovery queries and analysis.

This module provides caching capabilities for expensive operations
like SQL parsing, data profiling, and hierarchy extraction.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel


@dataclass
class CacheEntry:
    """Represents a cached value with metadata."""

    key: str
    value: Any
    created_at: float
    expires_at: float | None
    hit_count: int = 0
    last_accessed: float | None = None
    size_bytes: int = 0

    @property
    def is_expired(self) -> bool:
        """Check if the entry has expired."""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at


class ResultCache:
    """
    Result cache for discovery operations.

    Provides in-memory and optional SQLite-backed persistent caching
    for expensive operations like SQL parsing and analysis results.

    Example:
        cache = ResultCache()

        # Cache a parsed query
        cache.set("parsed_query_123", parsed_query, ttl=3600)

        # Retrieve later
        result = cache.get("parsed_query_123")
    """

    def __init__(
        self,
        max_memory_items: int = 1000,
        default_ttl: int = 3600,
        persist_path: str | None = None,
    ):
        """
        Initialize the result cache.

        Args:
            max_memory_items: Maximum items to keep in memory
            default_ttl: Default time-to-live in seconds
            persist_path: Path to SQLite file for persistence (optional)
        """
        self.max_memory_items = max_memory_items
        self.default_ttl = default_ttl
        self.persist_path = persist_path

        # In-memory cache
        self._memory_cache: dict[str, CacheEntry] = {}
        self._lock = threading.RLock()

        # Statistics
        self._hits = 0
        self._misses = 0
        self._evictions = 0

        # Initialize SQLite if persistence is enabled
        self._db: sqlite3.Connection | None = None
        if persist_path:
            self._init_sqlite(persist_path)

    def _init_sqlite(self, path: str) -> None:
        """Initialize SQLite database for persistence."""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                value TEXT,
                created_at REAL,
                expires_at REAL,
                hit_count INTEGER DEFAULT 0
            )
        """)
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_expires ON cache(expires_at)")
        self._db.commit()

    def _generate_key(self, *args: Any, **kwargs: Any) -> str:
        """Generate a cache key from arguments."""
        key_data = json.dumps(
            {"args": [str(a) for a in args], "kwargs": {k: str(v) for k, v in sorted(kwargs.items())}},
            sort_keys=True,
        )
        return hashlib.sha256(key_data.encode()).hexdigest()[:32]

    def get(self, key: str) -> Any | None:
        """
        Get a value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/expired
        """
        with self._lock:
            # Check memory cache first
            if key in self._memory_cache:
                entry = self._memory_cache[key]
                if entry.is_expired:
                    del self._memory_cache[key]
                    self._misses += 1
                    return None
                entry.hit_count += 1
                entry.last_accessed = time.time()
                self._hits += 1
                return entry.value

            # Check SQLite if available
            if self._db:
                cursor = self._db.execute(
                    "SELECT value, expires_at FROM cache WHERE key = ?",
                    (key,),
                )
                row = cursor.fetchone()
                if row:
                    value_json, expires_at = row
                    if expires_at and time.time() > expires_at:
                        self._db.execute("DELETE FROM cache WHERE key = ?", (key,))
                        self._db.commit()
                        self._misses += 1
                        return None

                    # Update hit count
                    self._db.execute(
                        "UPDATE cache SET hit_count = hit_count + 1 WHERE key = ?",
                        (key,),
                    )
                    self._db.commit()

                    self._hits += 1
                    return json.loads(value_json)

            self._misses += 1
            return None

    def set(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
        persist: bool = True,
    ) -> None:
        """
        Set a value in cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (None = use default)
            persist: Whether to persist to SQLite
        """
        if ttl is None:
            ttl = self.default_ttl

        now = time.time()
        expires_at = now + ttl if ttl > 0 else None

        # Serialize value for size estimation
        try:
            value_json = json.dumps(value, default=str)
            size_bytes = len(value_json.encode())
        except (TypeError, ValueError):
            value_json = str(value)
            size_bytes = len(value_json.encode())

        with self._lock:
            # Evict if at capacity
            if len(self._memory_cache) >= self.max_memory_items:
                self._evict_lru()

            # Store in memory
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=now,
                expires_at=expires_at,
                size_bytes=size_bytes,
            )
            self._memory_cache[key] = entry

            # Persist to SQLite if enabled
            if persist and self._db:
                try:
                    self._db.execute(
                        """
                        INSERT OR REPLACE INTO cache (key, value, created_at, expires_at, hit_count)
                        VALUES (?, ?, ?, ?, 0)
                        """,
                        (key, value_json, now, expires_at),
                    )
                    self._db.commit()
                except Exception:
                    pass  # Don't fail on persistence errors

    def delete(self, key: str) -> bool:
        """
        Delete a key from cache.

        Args:
            key: Cache key

        Returns:
            True if key was deleted
        """
        with self._lock:
            deleted = False

            if key in self._memory_cache:
                del self._memory_cache[key]
                deleted = True

            if self._db:
                cursor = self._db.execute("DELETE FROM cache WHERE key = ?", (key,))
                self._db.commit()
                if cursor.rowcount > 0:
                    deleted = True

            return deleted

    def clear(self) -> int:
        """
        Clear all cached values.

        Returns:
            Number of items cleared
        """
        with self._lock:
            count = len(self._memory_cache)
            self._memory_cache.clear()

            if self._db:
                cursor = self._db.execute("SELECT COUNT(*) FROM cache")
                count += cursor.fetchone()[0]
                self._db.execute("DELETE FROM cache")
                self._db.commit()

            return count

    def _evict_lru(self) -> None:
        """Evict least recently used entry from memory cache."""
        if not self._memory_cache:
            return

        # Find LRU entry
        lru_key = None
        lru_time = float("inf")

        for key, entry in self._memory_cache.items():
            access_time = entry.last_accessed or entry.created_at
            if access_time < lru_time:
                lru_time = access_time
                lru_key = key

        if lru_key:
            del self._memory_cache[lru_key]
            self._evictions += 1

    def cleanup_expired(self) -> int:
        """
        Remove all expired entries.

        Returns:
            Number of entries removed
        """
        with self._lock:
            now = time.time()
            removed = 0

            # Clean memory cache
            expired_keys = [
                key
                for key, entry in self._memory_cache.items()
                if entry.is_expired
            ]
            for key in expired_keys:
                del self._memory_cache[key]
                removed += 1

            # Clean SQLite
            if self._db:
                cursor = self._db.execute(
                    "DELETE FROM cache WHERE expires_at IS NOT NULL AND expires_at < ?",
                    (now,),
                )
                self._db.commit()
                removed += cursor.rowcount

            return removed

    def get_stats(self) -> dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        with self._lock:
            total_requests = self._hits + self._misses
            hit_rate = self._hits / total_requests if total_requests > 0 else 0.0

            memory_size = sum(e.size_bytes for e in self._memory_cache.values())

            stats = {
                "memory_items": len(self._memory_cache),
                "memory_size_bytes": memory_size,
                "memory_size_mb": round(memory_size / (1024 * 1024), 2),
                "max_memory_items": self.max_memory_items,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": round(hit_rate, 3),
                "evictions": self._evictions,
            }

            if self._db:
                cursor = self._db.execute("SELECT COUNT(*) FROM cache")
                stats["sqlite_items"] = cursor.fetchone()[0]

            return stats

    def get_or_compute(
        self,
        key: str,
        compute_fn: callable,
        ttl: int | None = None,
    ) -> Any:
        """
        Get value from cache or compute and cache it.

        Args:
            key: Cache key
            compute_fn: Function to compute value if not cached
            ttl: Time-to-live for cached value

        Returns:
            Cached or computed value
        """
        value = self.get(key)
        if value is not None:
            return value

        value = compute_fn()
        self.set(key, value, ttl=ttl)
        return value

    def close(self) -> None:
        """Close the cache and release resources."""
        if self._db:
            self._db.close()
            self._db = None


class CacheDecorator:
    """
    Decorator for caching function results.

    Example:
        cache = ResultCache()

        @CacheDecorator(cache, ttl=3600)
        def expensive_operation(arg1, arg2):
            return do_something(arg1, arg2)
    """

    def __init__(
        self,
        cache: ResultCache,
        ttl: int | None = None,
        key_prefix: str = "",
    ):
        """
        Initialize the cache decorator.

        Args:
            cache: ResultCache instance
            ttl: Time-to-live for cached results
            key_prefix: Prefix for cache keys
        """
        self.cache = cache
        self.ttl = ttl
        self.key_prefix = key_prefix

    def __call__(self, func: callable) -> callable:
        """Decorate a function with caching."""

        def wrapper(*args, **kwargs):
            # Generate cache key
            key = self.cache._generate_key(
                self.key_prefix,
                func.__name__,
                *args,
                **kwargs,
            )

            # Try to get from cache
            result = self.cache.get(key)
            if result is not None:
                return result

            # Compute and cache
            result = func(*args, **kwargs)
            self.cache.set(key, result, ttl=self.ttl)
            return result

        return wrapper
