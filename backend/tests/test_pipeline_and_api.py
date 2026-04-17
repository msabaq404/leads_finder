from __future__ import annotations

import json
import threading
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Any
import unittest

from backend.api.server import LeadsFinderApiServer
from backend.app import LeadsFinderApp
from backend.contracts.enums import LeadSource, LeadStatus
from backend.contracts.lead_schema import (
    DedupMetadata,
    ExtractedEntities,
    LeadRecord,
    LeadTrace,
    ManualReviewState,
)
from backend.ingestion.worker import IngestionRunSummary, SourceRunSummary
from backend.pipeline.engine import PipelineRunSummary
from backend.processing.dedup import DedupEngine
from backend.processing.filtering import ProgrammingTaskFilter
from backend.processing.ranking import LeadRanker
from backend.enrichment.service import LeadEnrichmentService
from backend.storage.repository import InMemoryLeadRepository
from backend.storage.service import PipelineStorageService
from backend.review.service import ReviewService


@dataclass(slots=True)
class FakeIngestionWorker:
    summary: IngestionRunSummary

    def run_once(self, from_time: datetime, to_time: datetime):
        return self.summary


class FakePipeline:
    def __init__(self, summary: PipelineRunSummary) -> None:
        self.summary = summary

    def run_once(self):
        return self.summary


def build_lead(
    lead_id: str = "lead-1",
    title: str = "Need help fixing a Python API integration bug",
    body: str = "We need a developer to debug a FastAPI integration issue with clear steps.",
    source: LeadSource = LeadSource.REDDIT,
    published_at: datetime | None = None,
) -> LeadRecord:
    now = datetime.utcnow()
    published_at = published_at or now - timedelta(hours=6)
    return LeadRecord(
        lead_id=lead_id,
        title=title,
        body=body,
        trace=LeadTrace(
            source=source,
            source_item_id=f"{lead_id}-source",
            source_url="https://example.com/lead",
            fetched_at=now,
            published_at=published_at,
            author_handle="alice",
            author_profile_url="https://example.com/alice",
        ),
        entities=ExtractedEntities(
            keywords=["python", "api", "bug", "debug", "integration"],
            languages=["python"],
            frameworks=["fastapi"],
            urgency_signals=["bug", "debug"],
            conversion_signals=["help wanted", "contract"],
        ),
        dedup=DedupMetadata(
            canonical_key=lead_id,
            content_hash=f"hash-{lead_id}",
            first_seen_at=now,
            last_seen_at=now,
        ),
        review=ManualReviewState(status=LeadStatus.NEW, csv_ready=True),
        raw_payload={"source": source.value},
        normalized_at=now,
    )


def build_pipeline_summary(lead: LeadRecord) -> PipelineRunSummary:
    now = datetime.utcnow()
    ingestion = IngestionRunSummary(
        started_at=now - timedelta(minutes=5),
        finished_at=now,
        leads=[lead],
        per_source=[
            SourceRunSummary(
                source=lead.trace.source.value,
                fetched_items=1,
                normalized_items=1,
                exhausted=True,
            )
        ],
    )
    task_filter = ProgrammingTaskFilter(min_confidence=0.1)
    dedup_engine = DedupEngine()
    ranker = LeadRanker()
    kept = [lead] if task_filter.evaluate(lead).accepted else []
    deduped = dedup_engine.deduplicate(kept)
    ranked = ranker.rank([outcome.canonical_lead for outcome in deduped])
    enrichment_service = LeadEnrichmentService(client=None)
    enriched = enrichment_service.enrich_ranked(ranked)
    return PipelineRunSummary(
        ingestion=ingestion,
        filtered_out=len(ingestion.leads) - len(kept),
        filtered_in=len(kept),
        deduped_groups=len(deduped),
        ranked=ranked,
        enriched=enriched,
    )


class LeadsFinderPipelineTests(unittest.TestCase):
    def test_pipeline_runs_end_to_end_with_fakes(self) -> None:
        lead = build_lead()
        ingestion_worker = FakeIngestionWorker(
            summary=IngestionRunSummary(
                started_at=datetime.utcnow() - timedelta(minutes=1),
                finished_at=datetime.utcnow(),
                leads=[lead],
                per_source=[
                    SourceRunSummary(
                        source=lead.trace.source.value,
                        fetched_items=1,
                        normalized_items=1,
                        exhausted=True,
                    )
                ],
            )
        )
        pipeline = __import__("backend.pipeline.engine", fromlist=["LeadPipeline"]).LeadPipeline(
            ingestion_worker=ingestion_worker,
            task_filter=ProgrammingTaskFilter(min_confidence=0.1),
            dedup_engine=DedupEngine(),
            ranker=LeadRanker(),
            enrichment_service=LeadEnrichmentService(client=None),
        )

        summary = pipeline.run_once()

        self.assertEqual(summary.filtered_in, 1)
        self.assertEqual(len(summary.ranked), 1)
        self.assertEqual(len(summary.enriched), 1)
        self.assertGreater(summary.ranked[0].breakdown.final_score, 0.0)
        self.assertTrue(summary.enriched[0].summary)


class LeadsFinderApiTests(unittest.TestCase):
    def test_http_api_serves_run_leads_and_export(self) -> None:
        lead = build_lead()
        summary = build_pipeline_summary(lead)
        fake_pipeline = FakePipeline(summary)
        repository = InMemoryLeadRepository()
        storage_service = PipelineStorageService(repository)
        review_service = ReviewService(repository)
        app = LeadsFinderApp(
            pipeline=fake_pipeline,
            repository=repository,
            storage_service=storage_service,
            review_service=review_service,
        )
        server = LeadsFinderApiServer(app=app, host="127.0.0.1", port=0)
        port = server._server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        try:
            self._assert_json_response(f"http://127.0.0.1:{port}/health", expected_status=200)
            html = self._fetch_text(f"http://127.0.0.1:{port}/")
            self.assertIn("Leads Finder Dashboard", html)
            run_payload = self._assert_json_response(
                f"http://127.0.0.1:{port}/api/run",
                method="POST",
                expected_status=HTTPStatus.ACCEPTED,
            )
            self.assertIn("persisted_run_id", run_payload)
            self.assertEqual(run_payload["stored_leads"], 1)

            leads_payload = self._assert_json_response(f"http://127.0.0.1:{port}/api/leads")
            self.assertEqual(leads_payload["count"], 1)
            self.assertEqual(leads_payload["items"][0]["title"], lead.title)

            runs_payload = self._assert_json_response(f"http://127.0.0.1:{port}/api/runs")
            self.assertEqual(runs_payload["count"], 1)

            csv_text = self._fetch_text(f"http://127.0.0.1:{port}/api/export.csv")
            self.assertIn("lead_id,title,source,score_total,status,summary,reasons", csv_text)
            self.assertIn(lead.lead_id, csv_text)
        finally:
            server.shutdown()
            thread.join(timeout=2)

    def _assert_json_response(
        self,
        url: str,
        method: str = "GET",
        expected_status: int = 200,
    ) -> dict[str, Any]:
        request = urllib.request.Request(url, method=method)
        with urllib.request.urlopen(request, timeout=10) as response:
            self.assertEqual(response.status, expected_status)
            return json.loads(response.read().decode("utf-8"))

    def _fetch_text(self, url: str) -> str:
        with urllib.request.urlopen(url, timeout=10) as response:
            self.assertEqual(response.status, 200)
            return response.read().decode("utf-8")


if __name__ == "__main__":
    unittest.main()
