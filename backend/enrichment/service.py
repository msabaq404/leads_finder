from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
import re
import time
from typing import Any

from backend.contracts.enums import LeadStatus
from backend.contracts.lead_schema import LeadRecord
from backend.processing.ranking import RankResult

from .cache import EnrichmentCache
from .gemini import GeminiBudget, GeminiEnricher
from .rapidapi_jobs import RapidApiJobsClient


@dataclass(slots=True)
class EnrichmentResult:
    lead: LeadRecord
    source: str
    summary: str
    category: str
    difficulty: str
    urgency: str
    tech_tags: list[str] = field(default_factory=list)
    confidence: float = 0.0
    cache_hit: bool = False


class LeadEnrichmentService:
    def __init__(
        self,
        client: GeminiEnricher | None = None,
        jobs_client: RapidApiJobsClient | None = None,
        cache: EnrichmentCache | None = None,
        budget: GeminiBudget | None = None,
        top_fraction: float = 0.2,
        missing_retry_attempts: int | None = None,
        missing_retry_backoff_seconds: float | None = None,
    ) -> None:
        self.client = client
        self.jobs_client = jobs_client
        self.cache = cache or EnrichmentCache()
        self.budget = budget or GeminiBudget()
        self.top_fraction = top_fraction
        self.batch_size = max(1, int(os.getenv("GEMINI_BATCH_SIZE", "10")))
        self.missing_retry_attempts = max(1, int(missing_retry_attempts or os.getenv("GEMINI_MISSING_RETRY_ATTEMPTS", "3")))
        self.missing_retry_backoff_seconds = max(
            0.0,
            float(missing_retry_backoff_seconds or os.getenv("GEMINI_MISSING_RETRY_BACKOFF_SECONDS", "0.25")),
        )
        self.rate_wait_timeout_seconds = max(0.0, float(os.getenv("GEMINI_RATE_WAIT_TIMEOUT_SECONDS", "65")))
        self.rate_wait_poll_seconds = max(0.01, float(os.getenv("GEMINI_RATE_WAIT_POLL_SECONDS", "0.25")))
        self.debug_gemini = os.getenv("GEMINI_DEBUG", "0").strip().lower() in {"1", "true", "yes", "on"}
        self.list_models_on_start = os.getenv("GEMINI_LIST_MODELS", "1").strip().lower() in {"1", "true", "yes", "on"}
        if self.debug_gemini:
            configured_model = getattr(self.client, "model_name", None) if self.client is not None else None
            print(
                "[GEMINI] debug enabled; "
                f"client_configured={self.client is not None}; "
                f"jobs_client_configured={self.jobs_client is not None}; "
                f"model={configured_model}; "
                f"daily_budget_remaining={max(self.budget.daily_request_limit - self.budget.requests_used, 0)}; "
                f"rpm_limit={self.budget.requests_per_minute_limit}; "
                f"batch_size={self.batch_size}"
            )
            # if self.client is not None and self.list_models_on_start and hasattr(self.client, "list_models"):
            #     try:
            #         models = self.client.list_models()
            #         if models:
            #             preview = "\n".join(models)
            #             # suffix = " ..." if len(models) > 20 else ""
            #             print(f"[GEMINI] available models ({len(models)}):\n {preview}")
            #         else:
            #             print("[GEMINI] available models: none returned")
            #     except Exception as error:
            #         print(f"[GEMINI] failed to list models: {error!r}")

    def enrich_ranked(self, ranked_leads: list[RankResult]) -> list[EnrichmentResult]:
        if not ranked_leads:
            if self.debug_gemini:
                print("[GEMINI] no ranked leads available for enrichment in this run.")
            return []

        limit = max(1, int(len(ranked_leads) * self.top_fraction))
        selected = ranked_leads[:limit]
        results: list[EnrichmentResult] = []

        if self.debug_gemini:
            print(
                "[GEMINI] enrichment selection: "
                f"ranked={len(ranked_leads)}, selected={len(selected)}, top_fraction={self.top_fraction}"
            )

        if self.client is None:
            if self.jobs_client is None:
                self._mark_failed([rank_result.lead for rank_result in selected], "Gemini client is not configured")
                return results
            return self._enrich_with_jobs_only([rank_result.lead for rank_result in selected])

        pending: list[LeadRecord] = []
        for rank_result in selected:
            lead = rank_result.lead
            cache_key = lead.dedup.content_hash
            cached = self.cache.get(cache_key)
            if cached is not None:
                if self.debug_gemini:
                    print(f"[GEMINI] cache hit for lead_id={lead.lead_id}; source={cached.get('source', 'gemini')}")
                results.append(self._build_result_from_cached(lead, cached))
                continue

            pending.append(lead)

        if pending:
            results.extend(self._enrich_pending_batch(pending))

        if self.jobs_client is not None:
            self._attach_jobs_enrichment([rank_result.lead for rank_result in selected])

        return results

    def _enrich_with_jobs_only(self, leads: list[LeadRecord]) -> list[EnrichmentResult]:
        if not leads or self.jobs_client is None:
            return []

        results: list[EnrichmentResult] = []
        for lead in leads:
            payload = self.jobs_client.enrich_lead(lead)
            if payload is None:
                raise RuntimeError(f"RapidAPI jobs enrichment returned no payload for lead {lead.lead_id}")
            lead.enrichment = payload
            lead.review.status = LeadStatus.SCORED

            job = payload.get("primary_job") if isinstance(payload, dict) else {}
            details = payload.get("job_details") if isinstance(payload, dict) else {}
            summary_parts = [str(job.get("title") or "").strip(), str(job.get("company_name") or "").strip()]
            details_summary = str(details.get("short_description") or "").strip()
            if details_summary:
                summary_parts.append(details_summary[:240])

            results.append(
                EnrichmentResult(
                    lead=lead,
                    source="rapidapi_jobs",
                    summary=" - ".join(part for part in summary_parts if part),
                    category="job",
                    difficulty="mid",
                    urgency="medium",
                    tech_tags=list(lead.entities.languages + lead.entities.frameworks)[:5],
                    confidence=0.65,
                    cache_hit=False,
                )
            )
        return results

    def _attach_jobs_enrichment(self, leads: list[LeadRecord]) -> None:
        assert self.jobs_client is not None
        for lead in leads:
            payload = self.jobs_client.enrich_lead(lead)
            if payload is None:
                raise RuntimeError(f"RapidAPI jobs enrichment returned no payload for lead {lead.lead_id}")
            merged = dict(lead.enrichment or {})
            merged["jobs"] = payload
            lead.enrichment = merged

    def _enrich_pending_batch(self, leads: list[LeadRecord]) -> list[EnrichmentResult]:
        assert self.client is not None
        results: list[EnrichmentResult] = []

        for batch in self._chunked(leads, self.batch_size):
            results.extend(self._enrich_batch_with_retries(batch))

        return results

    def _enrich_batch_with_retries(self, batch: list[LeadRecord]) -> list[EnrichmentResult]:
        assert self.client is not None
        remaining = list(batch)
        results: list[EnrichmentResult] = []
        attempt = 1

        while remaining:
            if not self.budget.wait_for_slot(
                max_wait_seconds=self.rate_wait_timeout_seconds,
                poll_interval_seconds=self.rate_wait_poll_seconds,
            ):
                if self.budget.is_daily_exhausted():
                    self._mark_failed(remaining, "Gemini daily budget exhausted")
                else:
                    self._mark_failed(
                        remaining,
                        "Gemini rate limit wait timeout",
                    )
                break

            if attempt > 1 and self.missing_retry_backoff_seconds > 0:
                delay = self.missing_retry_backoff_seconds * (2 ** (attempt - 2))
                time.sleep(delay)

            prompt = self._build_batch_prompt(remaining)
            response = self.client.enrich(prompt)
            self.budget.record_request()
            if self.debug_gemini:
                print("[GEMINI] raw batch response: " + json.dumps(response, ensure_ascii=True))
            by_id = self._batch_response_by_id(response)

            next_remaining: list[LeadRecord] = []
            for lead in remaining:
                payload = by_id.get(lead.lead_id)
                if payload is None:
                    next_remaining.append(lead)
                    continue

                normalized = self._normalize_response(lead, payload)
                self.cache.set(lead.dedup.content_hash, normalized)
                results.append(self._build_result_from_mapping(lead, normalized, cache_hit=False))

            if not next_remaining:
                break

            missing_ids = ", ".join(lead.lead_id for lead in next_remaining)
            if attempt >= self.missing_retry_attempts:
                self._mark_failed(next_remaining, f"Gemini batch response missing lead ids after retries: {missing_ids}")
                break

            if self.debug_gemini:
                print(
                    "[GEMINI] batch response missing lead ids; retrying "
                    f"attempt={attempt + 1}/{self.missing_retry_attempts}; missing={missing_ids}"
                )
            remaining = next_remaining
            attempt += 1

        return results

    def _chunked(self, values: list[LeadRecord], size: int) -> list[list[LeadRecord]]:
        return [values[index : index + size] for index in range(0, len(values), size)]

    def _build_batch_prompt(self, leads: list[LeadRecord]) -> str:
        items: list[str] = []
        for lead in leads:
            stack = ", ".join(lead.entities.languages + lead.entities.frameworks)
            keywords = ", ".join(lead.entities.keywords)
            items.append(
                "\n".join(
                    [
                        f"LEAD_ID: {lead.lead_id}",
                        f"TITLE: {lead.title}",
                        f"BODY: {lead.body}",
                        f"STACK: {stack}",
                        f"KEYWORDS: {keywords}",
                        f"SOURCE: {lead.trace.source.value}",
                    ]
                )
            )
        joined_items = "\n\n---\n\n".join(items)
        return (
            "Analyze each lead and return STRICT JSON with top-level key 'results' (array). "
            "Each result object MUST include keys: lead_id, is_help_request, is_hiring_request, is_freelancer_request, "
            "recommend_as_lead, lead_decision_reason, category, difficulty, urgency, tech_tags, summary, confidence. "
            "Set recommend_as_lead=true only if the author needs technical help or is hiring a programmer/freelancer/contractor. "
            "Reject posts from people seeking a job for themselves, open-to-work profiles, and onsite/in-office roles as leads.\n\n"
            f"LEADS:\n{joined_items}"
        )

    def _batch_response_by_id(self, response: dict[str, Any]) -> dict[str, dict[str, Any]]:
        results = response.get("results") if isinstance(response, dict) else None
        if not isinstance(results, list):
            return {}
        output: dict[str, dict[str, Any]] = {}
        for item in results:
            if not isinstance(item, dict):
                continue
            lead_id = str(item.get("lead_id") or "").strip()
            if lead_id:
                output[lead_id] = item
        return output

    def _build_result_from_cached(self, lead: LeadRecord, cached: dict[str, Any]) -> EnrichmentResult:
        lead.review.status = LeadStatus.SCORED
        result = self._build_result_from_mapping(lead, cached, cache_hit=True)
        lead.enrichment = cached
        return result

    def _build_result_from_mapping(
        self,
        lead: LeadRecord,
        data: dict[str, Any],
        cache_hit: bool,
    ) -> EnrichmentResult:
        lead.enrichment = data
        lead.review.status = LeadStatus.SCORED
        return EnrichmentResult(
            lead=lead,
            source=str(data.get("source") or "gemini"),
            summary=str(data.get("summary") or ""),
            category=str(data.get("category") or "other"),
            difficulty=str(data.get("difficulty") or "mid"),
            urgency=str(data.get("urgency") or "medium"),
            tech_tags=list(data.get("tech_tags") or []),
            confidence=float(data.get("confidence") or 0.0),
            cache_hit=cache_hit,
        )

    def _mark_failed(self, leads: list[LeadRecord], reason: str) -> None:
        for lead in leads:
            lead.review.status = LeadStatus.FAILED
            lead.enrichment = {
                "source": "gemini",
                "status": "failed",
                "failure_reason": reason,
            }

    def _build_prompt(self, lead: LeadRecord) -> str:
        stack = ", ".join(lead.entities.languages + lead.entities.frameworks)
        keywords = ", ".join(lead.entities.keywords)
        return (
            "Analyze this post and return strict JSON with keys "
            "is_help_request, is_hiring_request, is_freelancer_request, recommend_as_lead, lead_decision_reason, "
            "category, difficulty, urgency, tech_tags, summary, confidence.\n"
            "Rules: recommend_as_lead=true only if author needs technical help OR is hiring a programmer/freelancer/contractor. "
            "Reject self-seeking job posts, open-to-work profiles, and onsite/in-office roles as leads.\n\n"
            f"TITLE: {lead.title}\n"
            f"BODY: {lead.body}\n"
            f"STACK: {stack}\n"
            f"KEYWORDS: {keywords}\n"
            f"SOURCE: {lead.trace.source.value}\n"
        )

    def _normalize_response(self, lead: LeadRecord, response: dict[str, Any]) -> dict[str, Any]:
        tech_tags = response.get("tech_tags")
        if not isinstance(tech_tags, list):
            raise RuntimeError(f"Gemini returned invalid tech_tags for lead {lead.lead_id}")

        summary = response.get("summary")
        if not isinstance(summary, str) or not summary.strip():
            raise RuntimeError(f"Gemini returned invalid summary for lead {lead.lead_id}")

        return {
            "source": "gemini",
            "is_help_request": bool(response["is_help_request"]),
            "is_hiring_request": bool(response["is_hiring_request"]),
            "is_freelancer_request": bool(response["is_freelancer_request"]),
            "recommend_as_lead": bool(response["recommend_as_lead"]),
            "lead_decision_reason": str(response["lead_decision_reason"]),
            "category": str(response["category"]),
            "difficulty": str(response["difficulty"]),
            "urgency": str(response["urgency"]),
            "tech_tags": self._dedupe([str(value) for value in tech_tags]),
            "summary": summary,
            "confidence": self._clamp_confidence(response["confidence"]),
        }

    def _dedupe(self, values: list[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            normalized = value.strip().lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(normalized)
        return result

    def _clamp_confidence(self, value: Any) -> float:
        try:
            confidence = float(value)
        except (TypeError, ValueError):
            confidence = 0.0
        return max(0.0, min(confidence, 1.0))
