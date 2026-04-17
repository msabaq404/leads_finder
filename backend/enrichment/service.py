from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from backend.contracts.lead_schema import LeadRecord
from backend.processing.ranking import RankResult

from .cache import EnrichmentCache
from .gemini import GeminiBudget, GeminiEnricher


@dataclass(slots=True)
class EnrichmentResult:
    lead: LeadRecord
    source: str
    summary: str
    category: str
    difficulty: str
    urgency: str
    tech_tags: list[str] = field(default_factory=list)
    confidence: float = 0.0
    cache_hit: bool = False


class LeadEnrichmentService:
    def __init__(
        self,
        client: GeminiEnricher | None = None,
        cache: EnrichmentCache | None = None,
        budget: GeminiBudget | None = None,
        top_fraction: float = 0.2,
    ) -> None:
        self.client = client
        self.cache = cache or EnrichmentCache()
        self.budget = budget or GeminiBudget()
        self.top_fraction = top_fraction

    def enrich_ranked(self, ranked_leads: list[RankResult]) -> list[EnrichmentResult]:
        if not ranked_leads:
            return []

        limit = max(1, int(len(ranked_leads) * self.top_fraction))
        selected = ranked_leads[:limit]
        results: list[EnrichmentResult] = []

        for rank_result in selected:
            lead = rank_result.lead
            cache_key = lead.dedup.content_hash
            cached = self.cache.get(cache_key)
            if cached is not None:
                results.append(self._build_result_from_cached(lead, cached))
                continue

            if self.client is None or not self.budget.can_request():
                fallback = self._fallback_enrichment(lead)
                self.cache.set(cache_key, fallback)
                results.append(self._build_result_from_mapping(lead, fallback, cache_hit=False))
                continue

            prompt = self._build_prompt(lead)
            try:
                response = self.client.enrich(prompt)
                self.budget.record_request()
                normalized = self._normalize_response(lead, response)
                self.cache.set(cache_key, normalized)
                results.append(self._build_result_from_mapping(lead, normalized, cache_hit=False))
            except Exception:
                fallback = self._fallback_enrichment(lead)
                self.cache.set(cache_key, fallback)
                results.append(self._build_result_from_mapping(lead, fallback, cache_hit=False))

        return results

    def _build_result_from_cached(self, lead: LeadRecord, cached: dict[str, Any]) -> EnrichmentResult:
        result = self._build_result_from_mapping(lead, cached, cache_hit=True)
        lead.enrichment = cached
        return result

    def _build_result_from_mapping(
        self,
        lead: LeadRecord,
        data: dict[str, Any],
        cache_hit: bool,
    ) -> EnrichmentResult:
        lead.enrichment = data
        return EnrichmentResult(
            lead=lead,
            source=str(data.get("source") or "fallback"),
            summary=str(data.get("summary") or ""),
            category=str(data.get("category") or "other"),
            difficulty=str(data.get("difficulty") or "mid"),
            urgency=str(data.get("urgency") or "medium"),
            tech_tags=list(data.get("tech_tags") or []),
            confidence=float(data.get("confidence") or 0.0),
            cache_hit=cache_hit,
        )

    def _build_prompt(self, lead: LeadRecord) -> str:
        stack = ", ".join(lead.entities.languages + lead.entities.frameworks)
        keywords = ", ".join(lead.entities.keywords)
        return (
            "Analyze this programming-task lead and return strict JSON with keys "
            "category, difficulty, urgency, tech_tags, summary, confidence.\n\n"
            f"TITLE: {lead.title}\n"
            f"BODY: {lead.body}\n"
            f"STACK: {stack}\n"
            f"KEYWORDS: {keywords}\n"
            f"SOURCE: {lead.trace.source.value}\n"
        )

    def _fallback_enrichment(self, lead: LeadRecord) -> dict[str, Any]:
        tags = lead.entities.languages + lead.entities.frameworks + lead.entities.keywords[:3]
        return {
            "source": "fallback",
            "category": self._category_from_lead(lead),
            "difficulty": self._difficulty_from_lead(lead),
            "urgency": self._urgency_from_lead(lead),
            "tech_tags": self._dedupe(tags)[:8],
            "summary": self._summary_from_lead(lead),
            "confidence": 0.42,
        }

    def _normalize_response(self, lead: LeadRecord, response: dict[str, Any]) -> dict[str, Any]:
        normalized = self._fallback_enrichment(lead)
        normalized.update(
            {
                "source": "gemini",
                "category": str(response.get("category") or normalized["category"]),
                "difficulty": str(response.get("difficulty") or normalized["difficulty"]),
                "urgency": str(response.get("urgency") or normalized["urgency"]),
                "tech_tags": self._dedupe(response.get("tech_tags") or normalized["tech_tags"]),
                "summary": str(response.get("summary") or normalized["summary"]),
                "confidence": self._clamp_confidence(response.get("confidence")),
            }
        )
        return normalized

    def _category_from_lead(self, lead: LeadRecord) -> str:
        text = f"{lead.title} {lead.body}".lower()
        if any(term in text for term in ["bug", "error", "exception", "fix"]):
            return "bug_fix"
        if any(term in text for term in ["integration", "api", "connect", "webhook"]):
            return "integration"
        if any(term in text for term in ["refactor", "optimiz", "performance", "scale"]):
            return "optimization"
        if any(term in text for term in ["hire", "freelance", "contract", "consult"]):
            return "contracting"
        return "other"

    def _difficulty_from_lead(self, lead: LeadRecord) -> str:
        if len((lead.title + " " + lead.body).split()) < 40:
            return "junior"
        if len(lead.entities.keywords) > 6 or len(lead.entities.frameworks) > 1:
            return "senior"
        return "mid"

    def _urgency_from_lead(self, lead: LeadRecord) -> str:
        if lead.entities.urgency_signals:
            return "high"
        return "medium"

    def _summary_from_lead(self, lead: LeadRecord) -> str:
        text = f"{lead.title}. {lead.body}".strip()
        return text[:240]

    def _dedupe(self, values: list[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            normalized = value.strip().lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(normalized)
        return result

    def _clamp_confidence(self, value: Any) -> float:
        try:
            confidence = float(value)
        except (TypeError, ValueError):
            confidence = 0.0
        return max(0.0, min(confidence, 1.0))
