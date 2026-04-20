from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from enum import Enum
import json
from pathlib import Path
import sqlite3
from typing import Protocol

try:
    import pyodbc
except ImportError:  # pragma: no cover - dependency can be optional in local-only setups
    pyodbc = None


from backend.contracts.enums import LeadSource, LeadStatus
from backend.contracts.lead_schema import (
    DedupMetadata,
    ExtractedEntities,
    LeadRecord,
    LeadTrace,
    ManualReviewState,
)
from backend.contracts.ranking import LeadScoreBreakdown
from backend.enrichment.service import EnrichmentResult
from backend.ingestion.worker import IngestionRunSummary, SourceRunSummary
from backend.pipeline.engine import PipelineRunSummary
from backend.processing.ranking import RankResult


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

    def get_lead_ids(self) -> set[str]:
        """Return IDs for all stored leads."""

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

    def get_lead_ids(self) -> set[str]:
        return set(self._leads.keys())

    def get_pipeline_runs(self) -> list[StoredPipelineRun]:
        return list(self._runs)


class SQLiteLeadRepository:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_db(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS leads (
                    lead_id TEXT PRIMARY KEY,
                    score_total REAL,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    summary_json TEXT NOT NULL
                )
                """
            )
            connection.commit()

    def save_pipeline_run(self, run_id: str, summary: PipelineRunSummary) -> None:
        serialized_summary = _to_json(summary)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO pipeline_runs (run_id, created_at, summary_json)
                VALUES (?, ?, ?)
                """,
                (run_id, datetime.utcnow().isoformat(), serialized_summary),
            )
            connection.commit()

    def upsert_leads(self, leads: list[LeadRecord]) -> None:
        now = datetime.utcnow().isoformat()
        with self._connect() as connection:
            for lead in leads:
                connection.execute(
                    """
                    INSERT OR REPLACE INTO leads (lead_id, score_total, payload_json, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (lead.lead_id, lead.score_total, _to_json(lead), now),
                )
            connection.commit()

    def list_leads(self) -> list[LeadRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT payload_json
                FROM leads
                ORDER BY COALESCE(score_total, 0) DESC, updated_at DESC
                """
            ).fetchall()
        return [_lead_from_dict(json.loads(row["payload_json"])) for row in rows]

    def get_lead_ids(self) -> set[str]:
        with self._connect() as connection:
            rows = connection.execute("SELECT lead_id FROM leads").fetchall()
        return {str(row["lead_id"]) for row in rows}

    def get_pipeline_runs(self) -> list[StoredPipelineRun]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT run_id, created_at, summary_json
                FROM pipeline_runs
                ORDER BY created_at DESC
                """
            ).fetchall()
        return [
            StoredPipelineRun(
                run_id=row["run_id"],
                created_at=_parse_datetime(row["created_at"]) or datetime.utcnow(),
                summary=_pipeline_summary_from_dict(json.loads(row["summary_json"])),
            )
            for row in rows
        ]


class AzureSqlLeadRepository:
    def __init__(
        self,
        *,
        connection_string: str,
    ) -> None:
        if pyodbc is None:
            raise RuntimeError("pyodbc is not installed. Add pyodbc to requirements.")

        if not connection_string or not connection_string.strip():
            raise RuntimeError("AZURE_SQL_CONNECTION_STRING is required")

        self.connection_string = connection_string.strip()
        self._init_db()

    def _connect(self):
        return pyodbc.connect(self.connection_string, autocommit=False)

    def _init_db(self) -> None:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                IF OBJECT_ID('dbo.leads', 'U') IS NULL
                BEGIN
                    CREATE TABLE dbo.leads (
                        lead_id NVARCHAR(255) NOT NULL PRIMARY KEY,
                        score_total FLOAT NULL,
                        payload_json NVARCHAR(MAX) NOT NULL,
                        updated_at DATETIME2 NOT NULL
                    )
                END
                """
            )
            cursor.execute(
                """
                IF OBJECT_ID('dbo.pipeline_runs', 'U') IS NULL
                BEGIN
                    CREATE TABLE dbo.pipeline_runs (
                        run_id NVARCHAR(64) NOT NULL PRIMARY KEY,
                        created_at DATETIME2 NOT NULL,
                        summary_json NVARCHAR(MAX) NOT NULL
                    )
                END
                """
            )
            connection.commit()

    def save_pipeline_run(self, run_id: str, summary: PipelineRunSummary) -> None:
        serialized_summary = _to_json(summary)
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                MERGE dbo.pipeline_runs AS target
                USING (SELECT ? AS run_id, ? AS created_at, ? AS summary_json) AS source
                ON target.run_id = source.run_id
                WHEN MATCHED THEN
                    UPDATE SET created_at = source.created_at, summary_json = source.summary_json
                WHEN NOT MATCHED THEN
                    INSERT (run_id, created_at, summary_json)
                    VALUES (source.run_id, source.created_at, source.summary_json);
                """,
                (run_id, datetime.utcnow(), serialized_summary),
            )
            connection.commit()

    def upsert_leads(self, leads: list[LeadRecord]) -> None:
        now = datetime.utcnow()
        with self._connect() as connection:
            cursor = connection.cursor()
            for lead in leads:
                cursor.execute(
                    """
                    MERGE dbo.leads AS target
                    USING (SELECT ? AS lead_id, ? AS score_total, ? AS payload_json, ? AS updated_at) AS source
                    ON target.lead_id = source.lead_id
                    WHEN MATCHED THEN
                        UPDATE SET score_total = source.score_total, payload_json = source.payload_json, updated_at = source.updated_at
                    WHEN NOT MATCHED THEN
                        INSERT (lead_id, score_total, payload_json, updated_at)
                        VALUES (source.lead_id, source.score_total, source.payload_json, source.updated_at);
                    """,
                    (lead.lead_id, lead.score_total, _to_json(lead), now),
                )
            connection.commit()

    def list_leads(self) -> list[LeadRecord]:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT payload_json
                FROM dbo.leads
                ORDER BY ISNULL(score_total, 0) DESC, updated_at DESC
                """
            )
            rows = cursor.fetchall()
        return [_lead_from_dict(json.loads(row[0])) for row in rows]

    def get_lead_ids(self) -> set[str]:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT lead_id FROM dbo.leads")
            rows = cursor.fetchall()
        return {str(row[0]) for row in rows}

    def get_pipeline_runs(self) -> list[StoredPipelineRun]:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT run_id, created_at, summary_json
                FROM dbo.pipeline_runs
                ORDER BY created_at DESC
                """
            )
            rows = cursor.fetchall()
        return [
            StoredPipelineRun(
                run_id=str(row[0]),
                created_at=row[1] if isinstance(row[1], datetime) else (_parse_datetime(str(row[1])) or datetime.utcnow()),
                summary=_pipeline_summary_from_dict(json.loads(str(row[2]))),
            )
            for row in rows
        ]

def _to_primitive(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {key: _to_primitive(field_value) for key, field_value in asdict(value).items()}
    if isinstance(value, dict):
        return {key: _to_primitive(field_value) for key, field_value in value.items()}
    if isinstance(value, list):
        return [_to_primitive(item) for item in value]
    return value


def _to_json(value) -> str:
    return json.dumps(_to_primitive(value), ensure_ascii=True)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _lead_from_dict(data: dict) -> LeadRecord:
    trace_data = data.get("trace", {})
    entities_data = data.get("entities", {})
    dedup_data = data.get("dedup", {})
    review_data = data.get("review", {})
    return LeadRecord(
        lead_id=str(data.get("lead_id") or ""),
        title=str(data.get("title") or ""),
        body=str(data.get("body") or ""),
        trace=LeadTrace(
            source=LeadSource(trace_data.get("source", LeadSource.REDDIT.value)),
            source_item_id=str(trace_data.get("source_item_id") or ""),
            source_url=str(trace_data.get("source_url") or ""),
            fetched_at=_parse_datetime(trace_data.get("fetched_at")) or datetime.utcnow(),
            published_at=_parse_datetime(trace_data.get("published_at")),
            author_handle=trace_data.get("author_handle"),
            author_profile_url=trace_data.get("author_profile_url"),
        ),
        entities=ExtractedEntities(
            keywords=list(entities_data.get("keywords") or []),
            languages=list(entities_data.get("languages") or []),
            frameworks=list(entities_data.get("frameworks") or []),
            urgency_signals=list(entities_data.get("urgency_signals") or []),
            conversion_signals=list(entities_data.get("conversion_signals") or []),
        ),
        dedup=DedupMetadata(
            canonical_key=str(dedup_data.get("canonical_key") or ""),
            content_hash=str(dedup_data.get("content_hash") or ""),
            duplicate_of=dedup_data.get("duplicate_of"),
            merged_from_ids=list(dedup_data.get("merged_from_ids") or []),
            first_seen_at=_parse_datetime(dedup_data.get("first_seen_at")),
            last_seen_at=_parse_datetime(dedup_data.get("last_seen_at")),
            mention_count=int(dedup_data.get("mention_count") or 1),
        ),
        review=ManualReviewState(
            status=LeadStatus(review_data.get("status", LeadStatus.NEW.value)),
            reviewed_by=review_data.get("reviewed_by"),
            reviewed_at=_parse_datetime(review_data.get("reviewed_at")),
            reviewer_notes=review_data.get("reviewer_notes"),
            exported_at=_parse_datetime(review_data.get("exported_at")),
            export_batch_id=review_data.get("export_batch_id"),
            csv_ready=bool(review_data.get("csv_ready", False)),
        ),
        score_total=data.get("score_total"),
        score_breakdown=dict(data.get("score_breakdown") or {}),
        rank_reasons=list(data.get("rank_reasons") or []),
        enrichment=dict(data.get("enrichment") or {}),
        raw_payload=dict(data.get("raw_payload") or {}),
        normalized_at=_parse_datetime(data.get("normalized_at")) or datetime.utcnow(),
    )


def _rank_result_from_dict(data: dict) -> RankResult:
    breakdown_data = data.get("breakdown", {})
    breakdown = LeadScoreBreakdown(
        conversion_likelihood=float(breakdown_data.get("conversion_likelihood") or 0.0),
        urgency=float(breakdown_data.get("urgency") or 0.0),
        lead_quality=float(breakdown_data.get("lead_quality") or 0.0),
        final_score=float(breakdown_data.get("final_score") or 0.0),
    )
    return RankResult(
        lead=_lead_from_dict(data.get("lead") or {}),
        breakdown=breakdown,
        reasons=list(data.get("reasons") or []),
    )


def _enrichment_result_from_dict(data: dict) -> EnrichmentResult:
    return EnrichmentResult(
        lead=_lead_from_dict(data.get("lead") or {}),
        source=str(data.get("source") or "gemini"),
        summary=str(data.get("summary") or ""),
        category=str(data.get("category") or "other"),
        difficulty=str(data.get("difficulty") or "mid"),
        urgency=str(data.get("urgency") or "medium"),
        tech_tags=list(data.get("tech_tags") or []),
        confidence=float(data.get("confidence") or 0.0),
        cache_hit=bool(data.get("cache_hit", False)),
    )


def _pipeline_summary_from_dict(data: dict) -> PipelineRunSummary:
    ingestion_data = data.get("ingestion", {})
    per_source = [
        SourceRunSummary(
            source=str(item.get("source") or ""),
            fetched_items=int(item.get("fetched_items") or 0),
            normalized_items=int(item.get("normalized_items") or 0),
            next_cursor=item.get("next_cursor"),
            exhausted=bool(item.get("exhausted", False)),
            error=item.get("error"),
        )
        for item in (ingestion_data.get("per_source") or [])
    ]
    ingestion = IngestionRunSummary(
        started_at=_parse_datetime(ingestion_data.get("started_at")) or datetime.utcnow(),
        finished_at=_parse_datetime(ingestion_data.get("finished_at")) or datetime.utcnow(),
        leads=[_lead_from_dict(lead_data) for lead_data in (ingestion_data.get("leads") or [])],
        per_source=per_source,
    )
    return PipelineRunSummary(
        ingestion=ingestion,
        skipped_existing=int(data.get("skipped_existing") or 0),
        filtered_out=int(data.get("filtered_out") or 0),
        filtered_in=int(data.get("filtered_in") or 0),
        top_rejection_reasons=list(data.get("top_rejection_reasons") or []),
        deduped_groups=int(data.get("deduped_groups") or 0),
        ranked=[_rank_result_from_dict(item) for item in (data.get("ranked") or [])],
        enriched=[_enrichment_result_from_dict(item) for item in (data.get("enriched") or [])],
    )
