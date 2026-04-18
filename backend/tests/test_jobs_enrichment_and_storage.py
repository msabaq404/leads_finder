from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import unittest

from backend.contracts.enums import LeadSource, LeadStatus
from backend.contracts.lead_schema import (
    DedupMetadata,
    ExtractedEntities,
    LeadRecord,
    LeadTrace,
    ManualReviewState,
)
from backend.contracts.ranking import LeadScoreBreakdown
from backend.enrichment.service import LeadEnrichmentService
from backend.ingestion.worker import IngestionRunSummary, SourceRunSummary
from backend.pipeline.engine import PipelineRunSummary
from backend.processing.ranking import RankResult
from backend.storage.repository import InMemoryLeadRepository
from backend.storage.service import PipelineStorageService


@dataclass(slots=True)
class FakeJobsClient:
    def enrich_lead(self, lead: LeadRecord):
        return {
            "source": "rapidapi_jobs",
            "query": "python",
            "primary_job": {
                "job_id": "job-1",
                "title": "Python Developer",
                "company_name": "Aloha Protocol",
            },
            "related_jobs": [],
            "job_details": {
                "job_id": "job-1",
                "short_description": "Remote role",
            },
        }


def _build_lead(lead_id: str, source: LeadSource = LeadSource.X) -> LeadRecord:
    now = datetime.utcnow()
    return LeadRecord(
        lead_id=lead_id,
        title="Need help with Python API bug",
        body="Looking for someone to fix a FastAPI integration issue",
        trace=LeadTrace(
            source=source,
            source_item_id=f"{lead_id}-source",
            source_url=f"https://example.com/{lead_id}",
            fetched_at=now,
            published_at=now - timedelta(hours=2),
            author_handle="alice",
            author_profile_url=None,
        ),
        entities=ExtractedEntities(
            keywords=["python", "api", "bug", "fastapi"],
            languages=["python"],
            frameworks=["fastapi"],
            urgency_signals=["bug"],
            conversion_signals=["help"],
        ),
        dedup=DedupMetadata(canonical_key=lead_id, content_hash=f"hash-{lead_id}"),
        review=ManualReviewState(status=LeadStatus.NEW, csv_ready=True),
        raw_payload={"source": source.value},
        normalized_at=now,
    )


class JobsEnrichmentAndStorageTests(unittest.TestCase):
    def test_jobs_only_enrichment_produces_results(self) -> None:
        lead = _build_lead("lead-1")
        ranked = [
            RankResult(
                lead=lead,
                breakdown=LeadScoreBreakdown(0.8, 0.6, 0.7, 0.72),
                reasons=["high intent"],
            )
        ]

        service = LeadEnrichmentService(client=None, jobs_client=FakeJobsClient(), top_fraction=1.0)
        enriched = service.enrich_ranked(ranked)

        self.assertEqual(len(enriched), 1)
        self.assertEqual(enriched[0].source, "rapidapi_jobs")
        self.assertEqual(lead.review.status, LeadStatus.SCORED)
        self.assertEqual(lead.enrichment.get("source"), "rapidapi_jobs")

    def test_persist_run_stores_all_ingested_leads(self) -> None:
        lead_one = _build_lead("lead-1")
        lead_two = _build_lead("lead-2")
        ingestion = IngestionRunSummary(
            started_at=datetime.utcnow() - timedelta(minutes=3),
            finished_at=datetime.utcnow(),
            leads=[lead_one, lead_two],
            per_source=[
                SourceRunSummary(
                    source=LeadSource.X.value,
                    fetched_items=2,
                    normalized_items=2,
                    exhausted=True,
                )
            ],
        )

        summary = PipelineRunSummary(
            ingestion=ingestion,
            filtered_out=1,
            filtered_in=1,
            deduped_groups=1,
            ranked=[
                RankResult(
                    lead=lead_one,
                    breakdown=LeadScoreBreakdown(0.7, 0.6, 0.6, 0.63),
                    reasons=["kept"],
                )
            ],
            enriched=[],
        )

        repository = InMemoryLeadRepository()
        storage = PipelineStorageService(repository)
        persisted = storage.persist_run(summary)

        all_leads = repository.list_leads()
        self.assertEqual(persisted.stored_leads, 2)
        self.assertEqual(len(all_leads), 2)
        self.assertEqual({lead.lead_id for lead in all_leads}, {"lead-1", "lead-2"})


if __name__ == "__main__":
    unittest.main()
