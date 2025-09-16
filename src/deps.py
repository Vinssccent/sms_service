# src/deps.py
# -*- coding: utf-8 -*-
"""Common application dependencies."""
from __future__ import annotations

import logging
import os
from typing import Optional

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    redis = None  # type: ignore


log = logging.getLogger("src.deps")

_redis_client: Optional["redis.Redis"] = None  # type: ignore[name-defined]
_redis_initialized = False


def _as_int(value: str | None, default: int) -> int:
    try:
        return int(str(value).strip()) if value is not None and str(value).strip() else default
    except (TypeError, ValueError):
        return default


def _as_float(value: str | None, default: float) -> float:
    try:
        return float(str(value).strip()) if value is not None and str(value).strip() else default
    except (TypeError, ValueError):
        return default


def _init_redis() -> Optional["redis.Redis"]:  # type: ignore[name-defined]
    """Initialize the shared Redis client once."""
    global _redis_client, _redis_initialized

    if _redis_initialized:
        return _redis_client

    _redis_initialized = True

    if redis is None:  # type: ignore[truthy-bool]
        log.warning("Redis library is not installed; Redis integration disabled.")
        _redis_client = None
        return None

    host = os.getenv("REDIS_HOST", "127.0.0.1")
    port = _as_int(os.getenv("REDIS_PORT"), 6379)
    db_raw = os.getenv("REDIS_DB", "").strip()
    password = os.getenv("REDIS_PASSWORD", "").strip() or None
    socket_timeout = _as_float(os.getenv("REDIS_SOCKET_TIMEOUT"), 0.2)
    health_check_interval = _as_int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL"), 30)

    redis_kwargs: dict[str, object] = {
        "host": host,
        "port": port,
        "decode_responses": True,
        "socket_timeout": socket_timeout,
        "retry_on_timeout": True,
        "health_check_interval": health_check_interval,
    }

    if password:
        redis_kwargs["password"] = password

    if db_raw:
        try:
            redis_kwargs["db"] = int(db_raw)
        except ValueError:
            log.warning("Invalid REDIS_DB value '%s'; falling back to default database 0.", db_raw)

    ssl_raw = os.getenv("REDIS_SSL", "").strip().lower()
    if ssl_raw in {"1", "true", "yes", "on"}:
        redis_kwargs["ssl"] = True

    try:
        client = redis.Redis(**redis_kwargs)  # type: ignore[call-arg]
        client.ping()
        _redis_client = client
        log.info(
            "Redis connected (host=%s, port=%s, db=%s).",
            host,
            port,
            redis_kwargs.get("db", 0),
        )
    except Exception as exc:  # pragma: no cover - network dependent
        log.warning("Redis not available (%s) — continue without it.", exc)
        _redis_client = None

    return _redis_client


def get_redis(force_reconnect: bool = False) -> Optional["redis.Redis"]:  # type: ignore[name-defined]
    """Return a shared Redis client or ``None`` if Redis is unavailable."""
    global _redis_initialized, _redis_client

    if force_reconnect:
        _redis_initialized = False
        _redis_client = None

    if not _redis_initialized or _redis_client is None:
        _init_redis()

    return _redis_client


redis_client = get_redis()

__all__ = ["get_redis", "redis_client"]
