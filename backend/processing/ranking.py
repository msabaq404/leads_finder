from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

from backend.contracts.lead_schema import LeadRecord
from backend.contracts.ranking import LeadScoreBreakdown, RankingWeights, compute_balanced_score


@dataclass(slots=True)
class RankResult:
    lead: LeadRecord
    breakdown: LeadScoreBreakdown
    reasons: list[str] = field(default_factory=list)


class LeadRanker:
    def __init__(self, weights: RankingWeights | None = None) -> None:
        self.weights = weights or RankingWeights()

    def rank(self, leads: Iterable[LeadRecord]) -> list[RankResult]:
        results: list[RankResult] = []
        for lead in leads:
            breakdown, reasons = self.score_lead(lead)
            lead.score_total = breakdown.final_score
            lead.score_breakdown = {
                "conversion_likelihood": breakdown.conversion_likelihood,
                "urgency": breakdown.urgency,
                "lead_quality": breakdown.lead_quality,
                "final_score": breakdown.final_score,
            }
            lead.rank_reasons = reasons
            results.append(RankResult(lead=lead, breakdown=breakdown, reasons=reasons))
        results.sort(key=lambda result: result.breakdown.final_score, reverse=True)
        return results

    def score_lead(self, lead: LeadRecord) -> tuple[LeadScoreBreakdown, list[str]]:
        conversion = self._conversion_score(lead)
        urgency = self._urgency_score(lead)
        quality = self._quality_score(lead)
        breakdown = compute_balanced_score(conversion, urgency, quality, self.weights)
        reasons = self._reasons_for(lead, conversion, urgency, quality)
        return breakdown, reasons

    def _conversion_score(self, lead: LeadRecord) -> float:
        signals = lead.entities.conversion_signals
        keyword_bonus = min(len(signals) * 0.15, 0.45)
        recency_bonus = self._recency_bonus(lead)
        return round(min(0.4 + keyword_bonus + recency_bonus, 1.0), 3)

    def _urgency_score(self, lead: LeadRecord) -> float:
        signals = lead.entities.urgency_signals
        stack_bonus = 0.1 if lead.entities.languages or lead.entities.frameworks else 0.0
        return round(min(len(signals) * 0.2 + stack_bonus, 1.0), 3)

    def _quality_score(self, lead: LeadRecord) -> float:
        detail_score = 0.0
        text = f"{lead.title} {lead.body}"
        if len(text.split()) >= 20:
            detail_score += 0.25
        if lead.trace.source_url:
            detail_score += 0.15
        if lead.trace.author_handle:
            detail_score += 0.1
        if lead.entities.keywords:
            detail_score += 0.15
        if lead.dedup.content_hash:
            detail_score += 0.1
        if lead.score_total is not None:
            detail_score += 0.05
        return round(min(detail_score, 1.0), 3)

    def _recency_bonus(self, lead: LeadRecord) -> float:
        reference_time = lead.trace.published_at or lead.trace.fetched_at or lead.normalized_at
        age_hours = max((datetime.utcnow() - reference_time).total_seconds() / 3600.0, 0.0)
        if age_hours <= 12:
            return 0.2
        if age_hours <= 48:
            return 0.1
        if age_hours <= 168:
            return 0.05
        return 0.0

    def _reasons_for(
        self,
        lead: LeadRecord,
        conversion: float,
        urgency: float,
        quality: float,
    ) -> list[str]:
        reasons: list[str] = []
        if lead.entities.conversion_signals:
            reasons.append(f"conversion signals: {', '.join(lead.entities.conversion_signals[:4])}")
        if lead.entities.urgency_signals:
            reasons.append(f"urgency signals: {', '.join(lead.entities.urgency_signals[:4])}")
        if lead.entities.languages or lead.entities.frameworks:
            stack = lead.entities.languages + lead.entities.frameworks
            reasons.append(f"tech stack context: {', '.join(stack[:4])}")
        if lead.trace.published_at:
            reasons.append("recently published or tracked")
        reasons.append(f"component scores: conversion={conversion}, urgency={urgency}, quality={quality}")
        return reasons
