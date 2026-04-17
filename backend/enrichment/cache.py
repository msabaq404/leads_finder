from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any


@dataclass(slots=True)
class CacheEntry:
    value: dict[str, Any]
    created_at: datetime
    expires_at: datetime


class EnrichmentCache:
    def __init__(self, ttl_hours: int = 24) -> None:
        self.ttl = timedelta(hours=ttl_hours)
        self._entries: dict[str, CacheEntry] = {}

    def get(self, key: str) -> dict[str, Any] | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if datetime.utcnow() >= entry.expires_at:
            self._entries.pop(key, None)
            return None
        return entry.value

    def set(self, key: str, value: dict[str, Any]) -> None:
        now = datetime.utcnow()
        self._entries[key] = CacheEntry(value=value, created_at=now, expires_at=now + self.ttl)

    def has(self, key: str) -> bool:
        return self.get(key) is not None
