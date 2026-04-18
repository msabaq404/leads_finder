from __future__ import annotations

from typing import Any
from datetime import datetime

from backend.contracts.enums import LeadSource
from backend.contracts.source_adapter import SourceAdapterConfig

from .base import BaseSourceAdapter, NormalizedLeadParts
from .transport import HttpTransport, cursor_page, utc_to_epoch
from .utils import content_hash, parse_unix_datetime, split_words


class HackerNewsAdapter(BaseSourceAdapter):
    def __init__(self, config: SourceAdapterConfig, raw_fetcher=None) -> None:
        if config.source != LeadSource.HACKER_NEWS:
            raise ValueError("HackerNewsAdapter requires the hacker_news source")
        if raw_fetcher is None:
            raw_fetcher = self._build_default_fetcher(config)
        super().__init__(config=config, raw_fetcher=raw_fetcher)

    def _build_default_fetcher(self, config: SourceAdapterConfig):
        transport = HttpTransport()

        def fetcher(request):
            page, page_size = cursor_page(request, config.page_size)
            tags = "story"
            numeric_filters = f"created_at_i>={utc_to_epoch(request.from_time)}"
            intent_query = "job"
            payload = transport.get_json(
                "https://hn.algolia.com/api/v1/search_by_date",
                params={
                    "query": intent_query,
                    "tags": tags,
                    "numericFilters": numeric_filters,
                    "page": page - 1,
                    "hitsPerPage": page_size,
                },
                timeout=config.timeout_seconds,
            )
            items = payload.get("hits", [])
            exhausted = page - 1 >= int(payload.get("nbPages", 1)) - 1
            next_cursor = str(page + 1) if not exhausted else None
            return {"items": items, "next_cursor": next_cursor, "exhausted": exhausted}

        return fetcher

    def normalize_parts(self, raw_item: dict[str, Any]) -> NormalizedLeadParts:
        item_id = str(raw_item.get("id") or raw_item.get("objectID") or "hacker-news-item")
        title = str(raw_item.get("title") or raw_item.get("story_title") or "")
        body = str(raw_item.get("text") or raw_item.get("story_text") or raw_item.get("comment_text") or "")
        url = str(raw_item.get("url") or raw_item.get("story_url") or raw_item.get("link") or "")
        author = str(raw_item.get("by") or raw_item.get("author") or "")
        published_at = parse_unix_datetime(raw_item.get("time") or raw_item.get("created_at"))
        keywords = split_words(title + " " + body)
        text = f"{title} {body}".lower()
        conversion_signals: list[str] = []
        if "who is hiring" in text or "hiring" in keywords:
            conversion_signals.append("hiring")
        if "freelance" in keywords:
            conversion_signals.append("freelance")
        if "contract" in keywords:
            conversion_signals.append("contract")
        if "bounty" in keywords:
            conversion_signals.append("bounty")
        if "help" in keywords:
            conversion_signals.append("need help")
        urgency_signals = [signal for signal in keywords if signal in {"urgent", "help", "fix", "bug"}]
        return NormalizedLeadParts(
            lead_id=f"hacker_news:{item_id}",
            title=title,
            body=body,
            source_item_id=item_id,
            source_url=url,
            fetched_at=datetime.utcnow(),
            published_at=published_at,
            author_handle=author,
            author_profile_url=f"https://news.ycombinator.com/user?id={author}" if author else None,
            keywords=keywords,
            urgency_signals=urgency_signals,
            conversion_signals=conversion_signals,
            canonical_key=item_id,
            content_hash=content_hash(title, body),
            raw_payload=raw_item,
        )
