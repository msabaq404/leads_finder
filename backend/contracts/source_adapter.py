from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from .enums import LeadSource, SourceHealth
from .lead_schema import LeadRecord


@dataclass(slots=True)
class RateLimitConfig:
    requests_per_minute: int
    burst_limit: int = 1
    cooldown_seconds: int = 60


@dataclass(slots=True)
class RetryPolicy:
    max_attempts: int = 5
    base_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 60.0
    jitter_fraction: float = 0.2


@dataclass(slots=True)
class SourceAdapterConfig:
    source: LeadSource
    enabled: bool = True
    timeout_seconds: float = 15.0
    page_size: int = 50
    feature_flag: str | None = None
    rate_limit: RateLimitConfig = field(
        default_factory=lambda: RateLimitConfig(requests_per_minute=30)
    )
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)


@dataclass(slots=True)
class FetchPageRequest:
    from_time: datetime
    to_time: datetime
    cursor: str | None = None
    page_size: int | None = None


@dataclass(slots=True)
class FetchPageResult:
    source: LeadSource
    items: list[dict[str, Any]]
    next_cursor: str | None
    fetched_at: datetime
    exhausted: bool = False


@dataclass(slots=True)
class SourceHealthStatus:
    source: LeadSource
    state: SourceHealth
    last_success_at: datetime | None = None
    last_error: str | None = None
    consecutive_failures: int = 0
    cooldown_until: datetime | None = None


class SourceAdapter(Protocol):
    """Contract for all source-specific ingestion adapters."""

    config: SourceAdapterConfig

    def fetch_page(self, request: FetchPageRequest) -> FetchPageResult:
        """Fetch raw source items using a cursor-based page request."""

    def normalize_item(self, raw_item: dict[str, Any]) -> LeadRecord:
        """Normalize one source item into the canonical LeadRecord schema."""

    def health(self) -> SourceHealthStatus:
        """Return current adapter health for observability and circuit logic."""
