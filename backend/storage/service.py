from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from backend.pipeline.engine import PipelineRunSummary

from .repository import LeadRepository


@dataclass(slots=True)
class PersistenceResult:
    run_id: str
    stored_leads: int


class PipelineStorageService:
    def __init__(self, repository: LeadRepository) -> None:
        self.repository = repository

    def persist_run(self, summary: PipelineRunSummary) -> PersistenceResult:
        run_id = uuid4().hex
        self.repository.save_pipeline_run(run_id, summary)
        self.repository.upsert_leads([result.lead for result in summary.ranked])
        self.repository.upsert_leads([result.lead for result in summary.enriched])
        return PersistenceResult(run_id=run_id, stored_leads=len(summary.ranked))
