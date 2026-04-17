from __future__ import annotations

from typing import Any
from datetime import datetime

from backend.contracts.enums import LeadSource
from backend.contracts.source_adapter import SourceAdapterConfig

from .base import BaseSourceAdapter, NormalizedLeadParts
from .transport import HttpTransport, cursor_page
from .utils import content_hash, parse_datetime, split_words


class DevToAdapter(BaseSourceAdapter):
    def __init__(self, config: SourceAdapterConfig, raw_fetcher=None) -> None:
        if config.source != LeadSource.DEV_TO:
            raise ValueError("DevToAdapter requires the dev_to source")
        if raw_fetcher is None:
            raw_fetcher = self._build_default_fetcher(config)
        super().__init__(config=config, raw_fetcher=raw_fetcher)

    def _build_default_fetcher(self, config: SourceAdapterConfig):
        transport = HttpTransport(user_agent="LeadsFinder/1.0 DevTo")

        def fetcher(request):
            page, page_size = cursor_page(request, config.page_size)
            payload = transport.get_json(
                "https://dev.to/api/articles",
                params={"per_page": page_size, "page": page},
                timeout=config.timeout_seconds,
            )
            items = payload if isinstance(payload, list) else payload.get("items", payload.get("articles", []))
            exhausted = len(items) < page_size
            next_cursor = str(page + 1) if not exhausted else None
            return {"items": items, "next_cursor": next_cursor, "exhausted": exhausted}

        return fetcher

    def normalize_parts(self, raw_item: dict[str, Any]) -> NormalizedLeadParts:
        article_id = str(raw_item.get("id") or raw_item.get("slug") or "devto-article")
        title = str(raw_item.get("title") or "")
        body = str(raw_item.get("description") or raw_item.get("body_markdown") or "")
        url = str(raw_item.get("url") or raw_item.get("canonical_url") or "")
        author = str(raw_item.get("user") or raw_item.get("author") or "")
        published_at = parse_datetime(raw_item.get("published_at") or raw_item.get("created_at"))
        keywords = split_words(title + " " + body)
        conversion_signals = [signal for signal in keywords if signal in {"freelance", "contract", "hiring", "help"}]
        urgency_signals = [signal for signal in keywords if signal in {"urgent", "bug", "performance", "debug"}]
        return NormalizedLeadParts(
            lead_id=f"dev_to:{article_id}",
            title=title,
            body=body,
            source_item_id=article_id,
            source_url=url,
            fetched_at=datetime.utcnow(),
            published_at=published_at,
            author_handle=author,
            author_profile_url=None,
            keywords=keywords,
            urgency_signals=urgency_signals,
            conversion_signals=conversion_signals,
            canonical_key=article_id,
            content_hash=content_hash(title, body),
            raw_payload=raw_item,
        )
