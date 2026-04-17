from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from backend.enrichment.service import EnrichmentResult, LeadEnrichmentService
from backend.ingestion.worker import IngestionRunSummary, IngestionWorker
from backend.processing.dedup import DedupEngine, DedupOutcome
from backend.processing.filtering import FilterDecision, ProgrammingTaskFilter
from backend.processing.ranking import LeadRanker, RankResult


@dataclass(slots=True)
class PipelineRunSummary:
    ingestion: IngestionRunSummary
    filtered_out: int = 0
    filtered_in: int = 0
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

    def run_once(self, hours_back: int = 24) -> PipelineRunSummary:
        now = datetime.utcnow()
        from_time = now - timedelta(hours=hours_back)
        ingestion = self.ingestion_worker.run_once(from_time=from_time, to_time=now)

        kept = []
        filtered_out = 0
        for lead in ingestion.leads:
            decision = self.task_filter.evaluate(lead)
            lead.rank_reasons.extend(decision.reasons)
            if decision.accepted:
                kept.append(lead)
            else:
                filtered_out += 1

        deduped = self.dedup_engine.deduplicate(kept)
        canonical_leads = [outcome.canonical_lead for outcome in deduped]
        ranked = self.ranker.rank(canonical_leads)

        enriched: list[EnrichmentResult] = []
        if self.enrichment_service is not None:
            enriched = self.enrichment_service.enrich_ranked(ranked)

        return PipelineRunSummary(
            ingestion=ingestion,
            filtered_out=filtered_out,
            filtered_in=len(kept),
            deduped_groups=len(deduped),
            ranked=ranked,
            enriched=enriched,
        )
