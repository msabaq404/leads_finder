from __future__ import annotations

from dataclasses import dataclass

from .lead_schema import LeadRecord


@dataclass(slots=True)
class RankingWeights:
    conversion_likelihood: float = 0.4
    urgency: float = 0.3
    lead_quality: float = 0.3

    def validate(self) -> None:
        total = self.conversion_likelihood + self.urgency + self.lead_quality
        if abs(total - 1.0) > 0.0001:
            raise ValueError("RankingWeights must sum to 1.0")


@dataclass(slots=True)
class LeadScoreBreakdown:
    conversion_likelihood: float
    urgency: float
    lead_quality: float
    final_score: float


def compute_balanced_score(
    conversion_likelihood: float,
    urgency: float,
    lead_quality: float,
    weights: RankingWeights,
) -> LeadScoreBreakdown:
    """Compute balanced rank score using normalized component inputs (0 to 1)."""
    weights.validate()
    final_score = (
        conversion_likelihood * weights.conversion_likelihood
        + urgency * weights.urgency
        + lead_quality * weights.lead_quality
    )
    return LeadScoreBreakdown(
        conversion_likelihood=conversion_likelihood,
        urgency=urgency,
        lead_quality=lead_quality,
        final_score=round(final_score, 6),
    )


def attach_score(
    lead: LeadRecord,
    breakdown: LeadScoreBreakdown,
    reasons: list[str],
) -> LeadRecord:
    lead.score_total = breakdown.final_score
    lead.score_breakdown = {
        "conversion_likelihood": breakdown.conversion_likelihood,
        "urgency": breakdown.urgency,
        "lead_quality": breakdown.lead_quality,
        "final_score": breakdown.final_score,
    }
    lead.rank_reasons = reasons
    return lead
