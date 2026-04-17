from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from backend.contracts.lead_schema import LeadRecord
from backend.pipeline.engine import PipelineRunSummary


@dataclass(slots=True)
class StoredPipelineRun:
    run_id: str
    created_at: datetime
    summary: PipelineRunSummary


class LeadRepository(Protocol):
    def save_pipeline_run(self, run_id: str, summary: PipelineRunSummary) -> None:
        """Persist a pipeline execution summary."""

    def upsert_leads(self, leads: list[LeadRecord]) -> None:
        """Persist normalized or scored leads."""

    def list_leads(self) -> list[LeadRecord]:
        """Return all stored leads."""

    def get_pipeline_runs(self) -> list[StoredPipelineRun]:
        """Return historical pipeline runs."""


class InMemoryLeadRepository:
    def __init__(self) -> None:
        self._leads: dict[str, LeadRecord] = {}
        self._runs: list[StoredPipelineRun] = []

    def save_pipeline_run(self, run_id: str, summary: PipelineRunSummary) -> None:
        self._runs.append(
            StoredPipelineRun(run_id=run_id, created_at=datetime.utcnow(), summary=summary)
        )

    def upsert_leads(self, leads: list[LeadRecord]) -> None:
        for lead in leads:
            self._leads[lead.lead_id] = lead

    def list_leads(self) -> list[LeadRecord]:
        return sorted(
            self._leads.values(),
            key=lambda lead: lead.score_total if lead.score_total is not None else 0.0,
            reverse=True,
        )

    def get_pipeline_runs(self) -> list[StoredPipelineRun]:
        return list(self._runs)
