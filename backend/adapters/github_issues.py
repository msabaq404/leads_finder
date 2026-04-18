from __future__ import annotations

from typing import Any
from datetime import datetime
import os

from backend.contracts.enums import LeadSource
from backend.contracts.source_adapter import FetchPageRequest, SourceAdapterConfig

from .base import BaseSourceAdapter, NormalizedLeadParts
from .transport import HttpTransport, TransportError, cursor_page
from .utils import content_hash, parse_datetime


class GitHubIssuesAdapter(BaseSourceAdapter):
    def __init__(self, config: SourceAdapterConfig, raw_fetcher=None) -> None:
        if config.source != LeadSource.GITHUB_ISSUES:
            raise ValueError("GitHubIssuesAdapter requires the github_issues source")
        if raw_fetcher is None:
            raw_fetcher = self._build_default_fetcher(config)
        super().__init__(config=config, raw_fetcher=raw_fetcher)

    def fetch_page(self, request: FetchPageRequest):
        return super().fetch_page(request)

    def _build_default_fetcher(self, config: SourceAdapterConfig):
        token = os.getenv("GITHUB_TOKEN", "").strip()
        headers: dict[str, str] = {"Accept": "application/vnd.github+json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        transport = HttpTransport(default_headers=headers)

        def fetcher(request: FetchPageRequest) -> dict[str, Any]:
            page, page_size = cursor_page(request, config.page_size)
            intent_query = (
                f"type:issue is:open created:{request.from_time.date().isoformat()}..{request.to_time.date().isoformat()} "
                f'"help wanted" OR freelance OR contract'
            )
            payload = transport.get_json(
                "https://api.github.com/search/issues",
                params={"q": intent_query, "per_page": page_size, "page": page},
                timeout=config.timeout_seconds,
            )
            items = payload.get("items", [])
            exhausted = len(items) < page_size
            next_cursor = str(page + 1) if not exhausted else None
            return {"items": items, "next_cursor": next_cursor, "exhausted": exhausted}

        return fetcher

    def normalize_parts(self, raw_item: dict[str, Any]) -> NormalizedLeadParts:
        issue_id = str(raw_item.get("id") or raw_item.get("number") or raw_item.get("node_id") or "github-issue")
        title = str(raw_item.get("title") or "")
        body = str(raw_item.get("body") or raw_item.get("description") or "")
        url = str(raw_item.get("html_url") or raw_item.get("url") or "")
        published_at = parse_datetime(raw_item.get("created_at"))
        keywords = _collect_labels(raw_item)
        urgency_signals = [label for label in keywords if label in {"bug", "help wanted", "urgent"}]
        text = f"{title} {body}".lower()
        conversion_signals: list[str] = []
        if "help wanted" in text:
            conversion_signals.append("help wanted")
        if "freelance" in text:
            conversion_signals.append("freelance")
        if "contract" in text or "contractor" in text:
            conversion_signals.append("contract")
        if "hiring" in text or "hire" in text:
            conversion_signals.append("hiring")
        if "bounty" in text:
            conversion_signals.append("bounty")
        if not conversion_signals and (title or body):
            conversion_signals.extend(["public_issue", "open_work_item"])
        owner = raw_item.get("user") or {}
        return NormalizedLeadParts(
            lead_id=f"github_issues:{issue_id}",
            title=title,
            body=body,
            source_item_id=issue_id,
            source_url=url,
            fetched_at=datetime.utcnow(),
            published_at=published_at,
            author_handle=str(owner.get("login") or raw_item.get("author") or ""),
            author_profile_url=str(owner.get("html_url") or ""),
            keywords=keywords,
            languages=_collect_text_values(raw_item, "languages"),
            frameworks=_collect_text_values(raw_item, "frameworks"),
            urgency_signals=urgency_signals,
            conversion_signals=conversion_signals,
            canonical_key=issue_id,
            content_hash=content_hash(title, body),
            raw_payload=raw_item,
        )


def _collect_labels(raw_item: dict[str, Any]) -> list[str]:
    labels = raw_item.get("labels") or []
    result: list[str] = []
    for label in labels:
        if isinstance(label, dict):
            name = str(label.get("name") or "").strip().lower()
            if name:
                result.append(name)
        elif isinstance(label, str):
            result.append(label.strip().lower())
    return result


def _collect_text_values(raw_item: dict[str, Any], key: str) -> list[str]:
    values = raw_item.get(key) or []
    if isinstance(values, str):
        return [values]
    return [str(value).strip().lower() for value in values if str(value).strip()]
