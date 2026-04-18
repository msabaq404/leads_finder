from __future__ import annotations

import json
import os
from typing import Any

from backend.adapters.transport import HttpTransport, TransportError
from backend.contracts.lead_schema import LeadRecord


class RapidApiJobsClient:
    """Twitter (X) jobs enrichment client using twitter241 RapidAPI endpoints."""

    def __init__(
        self,
        transport: HttpTransport | None = None,
        *,
        api_key: str | None = None,
        api_host: str | None = None,
        timeout_seconds: float | None = None,
    ) -> None:
        self.api_key = (api_key or os.getenv("RAPIDAPI_TWITTER_KEY", "")).strip()
        self.api_host = (api_host or os.getenv("RAPIDAPI_TWITTER_HOST", "twitter241.p.rapidapi.com")).strip()
        self.timeout_seconds = float(timeout_seconds or os.getenv("RAPIDAPI_TIMEOUT_SECONDS", "15"))

        if not self.api_key:
            raise ValueError("RAPIDAPI_TWITTER_KEY environment variable is required for jobs enrichment")

        self.transport = transport or HttpTransport(
            default_headers={
                "x-rapidapi-key": self.api_key,
                "x-rapidapi-host": self.api_host,
            }
        )

        self.default_count = max(1, int(os.getenv("RAPIDAPI_JOBS_SEARCH_COUNT", "3")))
        self.default_location_id = os.getenv("RAPIDAPI_JOBS_LOCATION_ID", "").strip() or None
        self.default_location_type = os.getenv("RAPIDAPI_JOBS_LOCATION_TYPE", "remote,hybrid")
        self.default_employment_type = os.getenv("RAPIDAPI_JOBS_EMPLOYMENT_TYPE", "full_time,contract_to_hire")
        self.default_seniority_level = os.getenv("RAPIDAPI_JOBS_SENIORITY_LEVEL", "junior,mid_level,senior")

    def enrich_lead(self, lead: LeadRecord) -> dict[str, Any] | None:
        keyword = self._build_keyword(lead)
        jobs, _ = self.search_jobs(keyword=keyword, count=self.default_count)
        if not jobs:
            raise RuntimeError(f"No jobs found for keyword: {keyword}")

        primary_job = jobs[0]
        details = self.get_job_details(str(primary_job.get("job_id") or ""))

        related_jobs = [
            {
                "job_id": job.get("job_id"),
                "title": job.get("title"),
                "location": job.get("location"),
                "redirect_url": job.get("redirect_url"),
                "company_name": job.get("company_name"),
                "user_screen_name": job.get("user_screen_name"),
                "salary_min": job.get("salary_min"),
                "salary_max": job.get("salary_max"),
                "salary_currency_code": job.get("salary_currency_code"),
            }
            for job in jobs
        ]

        detail_payload = None
        if details:
            detail_payload = {
                "job_id": details.get("job_id"),
                "title": details.get("title"),
                "location": details.get("location"),
                "location_type": details.get("location_type"),
                "employment_type": details.get("employment_type"),
                "external_url": details.get("external_url"),
                "job_page_url": details.get("job_page_url"),
                "short_description": _decode_rich_text_json(details.get("short_description")),
                "description": _decode_rich_text_json(details.get("description")),
                "company_name": details.get("company_name"),
                "user_screen_name": details.get("user_screen_name"),
            }

        return {
            "source": "rapidapi_jobs",
            "query": keyword,
            "primary_job": related_jobs[0],
            "related_jobs": related_jobs,
            "job_details": detail_payload,
        }

    def search_jobs(self, keyword: str, count: int | None = None, cursor: str | None = None):
        params: dict[str, Any] = {
            "keyword": keyword,
            "count": str(count or self.default_count),
            "job_location_type": self.default_location_type,
            "employment_type": self.default_employment_type,
            "seniority_level": self.default_seniority_level,
        }
        if self.default_location_id:
            params["job_location_id"] = self.default_location_id
        if cursor:
            params["cursor"] = cursor

        payload = self._get_json("https://twitter241.p.rapidapi.com/jobs-search", params)
        items = payload["result"]["job_search"]["items_results"]
        jobs: list[dict[str, Any]] = []
        for item in items:
            job_result = item.get("result", {})
            core = job_result.get("core", {})
            jobs.append(
                {
                    "job_id": str(item.get("rest_id") or ""),
                    "title": core.get("title"),
                    "location": core.get("location"),
                    "redirect_url": core.get("redirect_url"),
                    "company_name": (
                        job_result.get("company_profile_results", {})
                        .get("result", {})
                        .get("core", {})
                        .get("name")
                    ),
                    "user_screen_name": (
                        job_result.get("user_results", {})
                        .get("result", {})
                        .get("legacy", {})
                        .get("screen_name")
                    ),
                    "salary_min": core.get("salary_min"),
                    "salary_max": core.get("salary_max"),
                    "salary_currency_code": core.get("salary_currency_code"),
                }
            )
        next_cursor = payload["result"]["job_search"].get("slice_info", {}).get("next_cursor")
        return jobs, next_cursor

    def get_job_details(self, job_id: str):
        if not job_id:
            raise ValueError("job_id is required")
        payload = self._get_json("https://twitter241.p.rapidapi.com/job-details", {"jobId": job_id})
        job_data = payload["result"]["jobData"]
        result = job_data["result"]
        core = result["core"]
        return {
            "job_id": str(job_data.get("rest_id") or ""),
            "title": core.get("title"),
            "location": core.get("location"),
            "location_type": core.get("location_type"),
            "employment_type": core.get("employment_type"),
            "external_url": core.get("external_url"),
            "job_page_url": core.get("job_page_url"),
            "short_description": core.get("short_description"),
            "description": core.get("job_description"),
            "company_name": (
                result.get("company_profile_results", {})
                .get("result", {})
                .get("core", {})
                .get("name")
            ),
            "user_screen_name": (
                result.get("user_results", {})
                .get("result", {})
                .get("legacy", {})
                .get("screen_name")
            ),
        }

    def _get_json(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        try:
            return self.transport.get_json(url, params=params, timeout=self.timeout_seconds)
        except Exception as error:
            raise TransportError(f"RapidAPI jobs call failed for {url}: {error}") from error

    def _build_keyword(self, lead: LeadRecord) -> str:
        terms: list[str] = []

        lowered_title = lead.title.lower()
        if "python" in lowered_title:
            terms.append("python")
        if "django" in lowered_title:
            terms.append("django")
        if "fastapi" in lowered_title:
            terms.append("fastapi")

        for value in lead.entities.languages + lead.entities.frameworks + lead.entities.keywords:
            normalized = str(value).strip().lower()
            if not normalized:
                continue
            if normalized in {"help", "urgent", "looking", "hire", "hiring", "contract", "freelance", "bug", "fix"}:
                continue
            if normalized not in terms:
                terms.append(normalized)
            if len(terms) >= 4:
                break

        if not terms:
            return "python developer"
        return " ".join(terms[:4])


def _decode_rich_text_json(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return text

    blocks = parsed.get("blocks") if isinstance(parsed, dict) else None
    if not isinstance(blocks, list):
        return text

    lines: list[str] = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        line = str(block.get("text") or "").strip()
        if line:
            lines.append(line)
    return "\n".join(lines) if lines else text
