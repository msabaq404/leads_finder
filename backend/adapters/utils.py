from __future__ import annotations

from datetime import datetime
from hashlib import sha256
from typing import Any


def split_words(text: str) -> list[str]:
    words: list[str] = []
    for token in text.lower().split():
        cleaned = token.strip(".,:;!?()[]{}<>\"'")
        if cleaned:
            words.append(cleaned)
    return words


def parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def parse_unix_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromtimestamp(float(value))
    except (TypeError, ValueError, OSError):
        return None


def content_hash(title: str, body: str) -> str:
    normalized = f"{title.strip().lower()}\n{body.strip().lower()}".encode("utf-8")
    return sha256(normalized).hexdigest()