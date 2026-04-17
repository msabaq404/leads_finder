from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

from backend.contracts.enums import LeadSource, SourceHealth
from backend.contracts.lead_schema import (
    DedupMetadata,
    ExtractedEntities,
    LeadRecord,
    LeadTrace,
)
from backend.contracts.source_adapter import (
    FetchPageRequest,
    FetchPageResult,
    SourceAdapterConfig,
    SourceHealthStatus,
)

RawFetcher = Callable[[FetchPageRequest], dict[str, Any]]


@dataclass(slots=True)
class NormalizedLeadParts:
    lead_id: str
    title: str
    body: str
    source_item_id: str
    source_url: str
    fetched_at: datetime
    published_at: datetime | None = None
    author_handle: str | None = None
    author_profile_url: str | None = None
    keywords: list[str] | None = None
    languages: list[str] | None = None
    frameworks: list[str] | None = None
    urgency_signals: list[str] | None = None
    conversion_signals: list[str] | None = None
    canonical_key: str | None = None
    content_hash: str | None = None
    raw_payload: dict[str, Any] | None = None


class BaseSourceAdapter(ABC):
    """Shared behavior for all source adapters."""

    def __init__(
        self,
        config: SourceAdapterConfig,
        raw_fetcher: RawFetcher | None = None,
    ) -> None:
        self.config = config
        self._raw_fetcher = raw_fetcher
        self._last_success_at: datetime | None = None
        self._last_error: str | None = None
        self._consecutive_failures = 0
        self._cooldown_until: datetime | None = None

    def fetch_page(self, request: FetchPageRequest) -> FetchPageResult:
        if not self.config.enabled:
            return FetchPageResult(
                source=self.config.source,
                items=[],
                next_cursor=request.cursor,
                fetched_at=datetime.utcnow(),
                exhausted=True,
            )

        if self._raw_fetcher is None:
            return FetchPageResult(
                source=self.config.source,
                items=[],
                next_cursor=request.cursor,
                fetched_at=datetime.utcnow(),
                exhausted=True,
            )

        try:
            payload = self._raw_fetcher(request)
        except Exception as error:
            self.record_failure(str(error))
            raise
        items = payload.get("items", [])
        next_cursor = payload.get("next_cursor")
        exhausted = bool(payload.get("exhausted", False))
        self._mark_success()
        return FetchPageResult(
            source=self.config.source,
            items=items,
            next_cursor=next_cursor,
            fetched_at=datetime.utcnow(),
            exhausted=exhausted,
        )

    def normalize_item(self, raw_item: dict[str, Any]) -> LeadRecord:
        parts = self.normalize_parts(raw_item)
        trace = LeadTrace(
            source=self.config.source,
            source_item_id=parts.source_item_id,
            source_url=parts.source_url,
            fetched_at=parts.fetched_at,
            published_at=parts.published_at,
            author_handle=parts.author_handle,
            author_profile_url=parts.author_profile_url,
        )
        entities = ExtractedEntities(
            keywords=parts.keywords or [],
            languages=parts.languages or [],
            frameworks=parts.frameworks or [],
            urgency_signals=parts.urgency_signals or [],
            conversion_signals=parts.conversion_signals or [],
        )
        dedup = DedupMetadata(
            canonical_key=parts.canonical_key or parts.lead_id,
            content_hash=parts.content_hash or parts.lead_id,
        )
        return LeadRecord(
            lead_id=parts.lead_id,
            title=parts.title,
            body=parts.body,
            trace=trace,
            entities=entities,
            dedup=dedup,
            raw_payload=parts.raw_payload or raw_item,
            normalized_at=datetime.utcnow(),
        )

    def health(self) -> SourceHealthStatus:
        return SourceHealthStatus(
            source=self.config.source,
            state=self._health_state(),
            last_success_at=self._last_success_at,
            last_error=self._last_error,
            consecutive_failures=self._consecutive_failures,
            cooldown_until=self._cooldown_until,
        )

    def record_failure(self, error_message: str) -> None:
        self._last_error = error_message
        self._consecutive_failures += 1
        self._cooldown_until = datetime.utcnow() + timedelta(
            seconds=self.config.rate_limit.cooldown_seconds
        )

    def record_success(self) -> None:
        self._mark_success()

    @abstractmethod
    def normalize_parts(self, raw_item: dict[str, Any]) -> NormalizedLeadParts:
        """Convert a source payload into the canonical lead parts."""

    def _mark_success(self) -> None:
        self._last_success_at = datetime.utcnow()
        self._last_error = None
        self._consecutive_failures = 0
        self._cooldown_until = None

    def _health_state(self) -> SourceHealth:
        if not self.config.enabled:
            return SourceHealth.OFFLINE
        if self._cooldown_until is not None and datetime.utcnow() < self._cooldown_until:
            return SourceHealth.COOLING_DOWN
        if self._cooldown_until is not None and datetime.utcnow() >= self._cooldown_until:
            if self._consecutive_failures > 0:
                return SourceHealth.RATE_LIMITED
        if self._consecutive_failures >= 3:
            return SourceHealth.DEGRADED
        return SourceHealth.HEALTHY
