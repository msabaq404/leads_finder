from __future__ import annotations

from dataclasses import dataclass
import os

from backend.enrichment.service import LeadEnrichmentService
from backend.enrichment.gemini import GeminiClient
from backend.ingestion.registry import build_default_registry
from backend.ingestion.worker import IngestionWorker
from backend.pipeline.engine import LeadPipeline
from backend.processing.dedup import DedupEngine
from backend.processing.filtering import ProgrammingTaskFilter
from backend.processing.ranking import LeadRanker
from backend.review.export import export_leads_to_csv
from backend.review.service import ReviewService
from backend.storage.repository import InMemoryLeadRepository
from backend.storage.service import PipelineStorageService


@dataclass(slots=True)
class LeadsFinderApp:
    pipeline: LeadPipeline
    repository: InMemoryLeadRepository
    storage_service: PipelineStorageService
    review_service: ReviewService

    def run_once(self):
        summary = self.pipeline.run_once()
        persisted = self.storage_service.persist_run(summary)
        return summary, persisted

    def export_current_review_csv(self) -> str:
        review_items = self.review_service.list_review_items()
        return export_leads_to_csv(review_items)

    def list_review_items(self):
        return self.review_service.list_review_items()


def build_app() -> LeadsFinderApp:
    registry = build_default_registry()
    ingestion_worker = IngestionWorker(registry)
    gemini_client = GeminiClient.from_env(
        model_name=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"),
        timeout_seconds=float(os.getenv("GEMINI_TIMEOUT_SECONDS", "30")),
    )
    pipeline = LeadPipeline(
        ingestion_worker=ingestion_worker,
        task_filter=ProgrammingTaskFilter(),
        dedup_engine=DedupEngine(),
        ranker=LeadRanker(),
        enrichment_service=LeadEnrichmentService(client=gemini_client),
    )
    repository = InMemoryLeadRepository()
    storage_service = PipelineStorageService(repository)
    review_service = ReviewService(repository)
    return LeadsFinderApp(
        pipeline=pipeline,
        repository=repository,
        storage_service=storage_service,
        review_service=review_service,
    )
