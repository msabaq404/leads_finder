from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .enums import LeadSource, LeadStatus


@dataclass(slots=True)
class LeadTrace:
    source: LeadSource
    source_item_id: str
    source_url: str
    fetched_at: datetime
    published_at: datetime | None = None
    author_handle: str | None = None
    author_profile_url: str | None = None


@dataclass(slots=True)
class ExtractedEntities:
    keywords: list[str] = field(default_factory=list)
    languages: list[str] = field(default_factory=list)
    frameworks: list[str] = field(default_factory=list)
    urgency_signals: list[str] = field(default_factory=list)
    conversion_signals: list[str] = field(default_factory=list)


@dataclass(slots=True)
class DedupMetadata:
    canonical_key: str
    content_hash: str
    duplicate_of: str | None = None
    merged_from_ids: list[str] = field(default_factory=list)
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    mention_count: int = 1


@dataclass(slots=True)
class ManualReviewState:
    status: LeadStatus = LeadStatus.NEW
    reviewed_by: str | None = None
    reviewed_at: datetime | None = None
    reviewer_notes: str | None = None
    exported_at: datetime | None = None
    export_batch_id: str | None = None
    csv_ready: bool = False


@dataclass(slots=True)
class LeadRecord:
    lead_id: str
    title: str
    body: str
    trace: LeadTrace
    entities: ExtractedEntities
    dedup: DedupMetadata
    review: ManualReviewState = field(default_factory=ManualReviewState)
    score_total: float | None = None
    score_breakdown: dict[str, float] = field(default_factory=dict)
    rank_reasons: list[str] = field(default_factory=list)
    enrichment: dict[str, Any] = field(default_factory=dict)
    raw_payload: dict[str, Any] = field(default_factory=dict)
    normalized_at: datetime = field(default_factory=datetime.utcnow)
