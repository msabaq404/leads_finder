from __future__ import annotations

from dataclasses import dataclass, field
import inspect
import os
from pathlib import Path
import threading

from backend.enrichment.service import LeadEnrichmentService
from backend.enrichment.gemini import GeminiBudget, GeminiClient
from backend.enrichment.rapidapi_jobs import RapidApiJobsClient
from backend.ingestion.registry import build_default_registry
from backend.ingestion.worker import IngestionWorker
from backend.pipeline.engine import LeadPipeline
from backend.processing.dedup import DedupEngine
from backend.processing.filtering import ProgrammingTaskFilter
from backend.processing.ranking import LeadRanker
from backend.review.export import export_leads_to_csv
from backend.review.service import ReviewService
from backend.storage.repository import AzureSqlLeadRepository, LeadRepository, SQLiteLeadRepository
from backend.storage.service import PipelineStorageService


@dataclass(slots=True)
class LeadsFinderApp:
    pipeline: LeadPipeline
    repository: LeadRepository
    storage_service: PipelineStorageService
    review_service: ReviewService
    _run_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False, compare=False)

    def run_once(self):
        with self._run_lock:
            known_lead_ids = self.repository.get_lead_ids()
            pipeline_run = self.pipeline.run_once
            signature = inspect.signature(pipeline_run)
            kwargs = {}
            if "known_lead_ids" in signature.parameters:
                kwargs["known_lead_ids"] = known_lead_ids
            if "hours_back" in signature.parameters:
                try:
                    kwargs["hours_back"] = max(1, int(os.getenv("LEADS_HOURS_BACK", "168")))
                except ValueError:
                    kwargs["hours_back"] = 168
            summary = pipeline_run(**kwargs)
            persisted = self.storage_service.persist_run(summary)
            return summary, persisted

    def export_current_review_csv(self) -> str:
        review_items = self.review_service.list_review_items()
        return export_leads_to_csv(review_items)

    def list_review_items(self):
        return self.review_service.list_review_items()


def _load_local_env() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def build_app() -> LeadsFinderApp:
    _load_local_env()
    registry = build_default_registry()
    ingestion_worker = IngestionWorker(registry)
    min_confidence = float(os.getenv("LEADS_MIN_CONFIDENCE", "0.35"))
    model_name = os.getenv("GEMINI_MODEL", GeminiClient.model_name)
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    if not gemini_api_key:
        raise RuntimeError("GEMINI_API_KEY is not configured")
    gemini_client = GeminiClient(
        model_name=model_name,
        timeout_seconds=float(os.getenv("GEMINI_TIMEOUT_SECONDS", "30")),
        api_key=gemini_api_key,
    )
    available_models = gemini_client.list_models()
    if model_name not in available_models and f"models/{model_name}" not in available_models:
        raise RuntimeError(f"GEMINI_MODEL {model_name!r} is not available")
    gemini_budget = GeminiBudget(
        daily_request_limit=int(os.getenv("GEMINI_DAILY_REQUEST_LIMIT", "20")),
        requests_per_minute_limit=int(os.getenv("GEMINI_REQUESTS_PER_MINUTE", "5")),
    )

    jobs_enrichment_enabled = os.getenv("LEADS_ENABLE_RAPIDAPI_JOBS_ENRICHMENT", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    jobs_client = RapidApiJobsClient() if jobs_enrichment_enabled else None

    pipeline = LeadPipeline(
        ingestion_worker=ingestion_worker,
        task_filter=ProgrammingTaskFilter(min_confidence=min_confidence),
        dedup_engine=DedupEngine(),
        ranker=LeadRanker(),
        enrichment_service=LeadEnrichmentService(
            client=gemini_client,
            jobs_client=jobs_client,
            budget=gemini_budget,
            top_fraction=float(os.getenv("LEADS_ENRICH_TOP_FRACTION", "1.0")),
        ),
    )
    db_backend = os.getenv("LEADS_DB_BACKEND", "sqlite").strip().lower()
    azure_sql_connection_string = os.getenv("AZURE_SQL_CONNECTION_STRING", "").strip()

    if db_backend == "azure_sql" or azure_sql_connection_string:
        if not azure_sql_connection_string:
            raise RuntimeError("Azure SQL is enabled but AZURE_SQL_CONNECTION_STRING is missing")
        repository = AzureSqlLeadRepository(connection_string=azure_sql_connection_string)
    else:
        db_path = Path(os.getenv("LEADS_DB_PATH", "leads_finder.db"))
        repository = SQLiteLeadRepository(db_path)

    storage_service = PipelineStorageService(repository)
    review_service = ReviewService(repository)
    return LeadsFinderApp(
        pipeline=pipeline,
        repository=repository,
        storage_service=storage_service,
        review_service=review_service,
    )
