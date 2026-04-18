from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from backend.contracts.lead_schema import LeadRecord
from backend.storage.repository import LeadRepository


@dataclass(slots=True)
class ReviewItem:
    lead_id: str
    title: str
    source: str
    source_url: str
    score_total: float
    status: str
    reasons: list[str] = field(default_factory=list)
    summary: str = ""
    enrichment: dict[str, Any] = field(default_factory=dict)


class ReviewService:
    def __init__(self, repository: LeadRepository) -> None:
        self.repository = repository

    def list_review_items(self) -> list[ReviewItem]:
        items: list[ReviewItem] = []
        for lead in self.repository.list_leads():
            items.append(self._to_review_item(lead))
        return items

    def get_lead(self, lead_id: str) -> LeadRecord | None:
        for lead in self.repository.list_leads():
            if lead.lead_id == lead_id:
                return lead
        return None

    def _to_review_item(self, lead: LeadRecord) -> ReviewItem:
        enrichment = lead.enrichment or {}
        summary = str(enrichment.get("summary") or lead.body[:240])
        return ReviewItem(
            lead_id=lead.lead_id,
            title=lead.title,
            source=lead.trace.source.value,
            source_url=lead.trace.source_url,
            score_total=float(lead.score_total or 0.0),
            status=lead.review.status.value,
            reasons=list(lead.rank_reasons),
            summary=summary,
            enrichment=lead.enrichment,
        )
