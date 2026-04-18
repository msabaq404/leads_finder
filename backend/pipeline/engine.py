from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import Counter

from backend.enrichment.service import EnrichmentResult, LeadEnrichmentService
from backend.ingestion.worker import IngestionRunSummary, IngestionWorker
from backend.processing.dedup import DedupEngine, DedupOutcome
from backend.processing.filtering import FilterDecision, ProgrammingTaskFilter
from backend.processing.ranking import LeadRanker, RankResult


@dataclass(slots=True)
class PipelineRunSummary:
    ingestion: IngestionRunSummary
    skipped_existing: int = 0
    filtered_out: int = 0
    filtered_in: int = 0
    top_rejection_reasons: list[str] = field(default_factory=list)
    deduped_groups: int = 0
    ranked: list[RankResult] = field(default_factory=list)
    enriched: list[EnrichmentResult] = field(default_factory=list)


class LeadPipeline:
    def __init__(
        self,
        ingestion_worker: IngestionWorker,
        task_filter: ProgrammingTaskFilter | None = None,
        dedup_engine: DedupEngine | None = None,
        ranker: LeadRanker | None = None,
        enrichment_service: LeadEnrichmentService | None = None,
    ) -> None:
        self.ingestion_worker = ingestion_worker
        self.task_filter = task_filter or ProgrammingTaskFilter()
        self.dedup_engine = dedup_engine or DedupEngine()
        self.ranker = ranker or LeadRanker()
        self.enrichment_service = enrichment_service

    def run_once(
        self,
        hours_back: int = 24,
        known_lead_ids: set[str] | None = None,
    ) -> PipelineRunSummary:
        now = datetime.utcnow()
        from_time = now - timedelta(hours=hours_back)
        ingestion = self.ingestion_worker.run_once(from_time=from_time, to_time=now)
        known_lead_ids = known_lead_ids or set()

        kept = []
        filtered_out = 0
        skipped_existing = 0
        rejection_counter: Counter[str] = Counter()
        for lead in ingestion.leads:
            if lead.lead_id in known_lead_ids:
                skipped_existing += 1
                lead.rank_reasons.append("already present in database")
                continue
            decision = self.task_filter.evaluate(lead)
            lead.rank_reasons.extend(decision.reasons)
            if decision.accepted:
                kept.append(lead)
            else:
                filtered_out += 1
                reason = self._best_rejection_reason(decision)
                rejection_counter[reason] += 1

        deduped = self.dedup_engine.deduplicate(kept)
        canonical_leads = [outcome.canonical_lead for outcome in deduped]
        ranked = self.ranker.rank(canonical_leads)

        enriched: list[EnrichmentResult] = []
        if self.enrichment_service is not None:
            enriched = self.enrichment_service.enrich_ranked(ranked)

        return PipelineRunSummary(
            ingestion=ingestion,
            skipped_existing=skipped_existing,
            filtered_out=filtered_out,
            filtered_in=len(ranked),
            top_rejection_reasons=[f"{reason} ({count})" for reason, count in rejection_counter.most_common(5)],
            deduped_groups=len(deduped),
            ranked=ranked,
            enriched=enriched,
        )

    def _best_rejection_reason(self, decision: FilterDecision) -> str:
        if not decision.reasons:
            return "rejected by filter"
        priority_tokens = (
            "rejected",
            "missing",
            "too short",
            "insufficient",
        )
        for reason in decision.reasons:
            if any(token in reason.lower() for token in priority_tokens):
                return reason
        return decision.reasons[-1]
