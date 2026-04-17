from __future__ import annotations

from typing import Any
from datetime import datetime

from backend.contracts.enums import LeadSource
from backend.contracts.source_adapter import SourceAdapterConfig

from .base import BaseSourceAdapter, NormalizedLeadParts
from .transport import HttpTransport, cursor_page
from .utils import content_hash, parse_unix_datetime, split_words


class RedditAdapter(BaseSourceAdapter):
    def __init__(self, config: SourceAdapterConfig, raw_fetcher=None) -> None:
        if config.source != LeadSource.REDDIT:
            raise ValueError("RedditAdapter requires the reddit source")
        if raw_fetcher is None:
            raw_fetcher = self._build_default_fetcher(config)
        super().__init__(config=config, raw_fetcher=raw_fetcher)

    def _build_default_fetcher(self, config: SourceAdapterConfig):
        transport = HttpTransport(user_agent="LeadsFinder/1.0 Reddit")

        def fetcher(request):
            page, page_size = cursor_page(request, config.page_size)
            query = (
                f"(freelance OR contract OR hiring OR bug OR help wanted OR developer OR programmer) "
                f"timestamp:{int(request.from_time.timestamp())}..{int(request.to_time.timestamp())}"
            )
            payload = transport.get_json(
                "https://www.reddit.com/search.json",
                params={"q": query, "sort": "new", "limit": page_size, "page": page},
                timeout=config.timeout_seconds,
            )
            children = payload.get("data", {}).get("children", [])
            items = [child.get("data", child) for child in children]
            exhausted = len(items) < page_size
            next_cursor = str(page + 1) if not exhausted else None
            return {"items": items, "next_cursor": next_cursor, "exhausted": exhausted}

        return fetcher

    def normalize_parts(self, raw_item: dict[str, Any]) -> NormalizedLeadParts:
        post_id = str(raw_item.get("id") or raw_item.get("name") or "reddit-post")
        title = str(raw_item.get("title") or "")
        body = str(raw_item.get("selftext") or raw_item.get("body") or "")
        url = str(raw_item.get("url") or raw_item.get("permalink") or "")
        author = str(raw_item.get("author") or "")
        published_at = parse_unix_datetime(raw_item.get("created_utc") or raw_item.get("created_at"))
        keywords = split_words(title + " " + body)
        conversion_signals = [signal for signal in keywords if signal in {"freelance", "contract", "hiring", "need"}]
        urgency_signals = [signal for signal in keywords if signal in {"urgent", "asap", "bug", "help"}]
        return NormalizedLeadParts(
            lead_id=f"reddit:{post_id}",
            title=title,
            body=body,
            source_item_id=post_id,
            source_url=url,
            fetched_at=datetime.utcnow(),
            published_at=published_at,
            author_handle=author,
            author_profile_url=f"https://www.reddit.com/user/{author}" if author else None,
            keywords=keywords,
            urgency_signals=urgency_signals,
            conversion_signals=conversion_signals,
            canonical_key=post_id,
            content_hash=content_hash(title, body),
            raw_payload=raw_item,
        )
