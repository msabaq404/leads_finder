from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from backend.adapters.base import BaseSourceAdapter
from backend.contracts.lead_schema import LeadRecord
from backend.contracts.source_adapter import FetchPageRequest

from .registry import SourceRegistry


@dataclass(slots=True)
class SourceRunSummary:
    source: str
    fetched_items: int = 0
    normalized_items: int = 0
    next_cursor: str | None = None
    exhausted: bool = False
    error: str | None = None


@dataclass(slots=True)
class IngestionRunSummary:
    started_at: datetime
    finished_at: datetime
    leads: list[LeadRecord] = field(default_factory=list)
    per_source: list[SourceRunSummary] = field(default_factory=list)


class IngestionWorker:
    def __init__(self, registry: SourceRegistry) -> None:
        self.registry = registry

    def run_once(
        self,
        from_time: datetime,
        to_time: datetime,
        cursors: dict[str, str | None] | None = None,
    ) -> IngestionRunSummary:
        started_at = datetime.utcnow()
        leads: list[LeadRecord] = []
        per_source: list[SourceRunSummary] = []
        cursor_map = cursors or {}

        for adapter in self.registry.enabled_adapters():
            assert isinstance(adapter, BaseSourceAdapter)
            request = FetchPageRequest(
                from_time=from_time,
                to_time=to_time,
                cursor=cursor_map.get(adapter.config.source.value),
                page_size=adapter.config.page_size,
            )
            page = adapter.fetch_page(request)
            normalized = [adapter.normalize_item(item) for item in page.items]
            leads.extend(normalized)
            per_source.append(
                SourceRunSummary(
                    source=adapter.config.source.value,
                    fetched_items=len(page.items),
                    normalized_items=len(normalized),
                    next_cursor=page.next_cursor,
                    exhausted=page.exhausted,
                )
            )

        return IngestionRunSummary(
            started_at=started_at,
            finished_at=datetime.utcnow(),
            leads=leads,
            per_source=per_source,
        )