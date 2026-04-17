from __future__ import annotations

from typing import Any
from datetime import datetime

from backend.contracts.enums import LeadSource
from backend.contracts.source_adapter import SourceAdapterConfig

from .base import BaseSourceAdapter, NormalizedLeadParts
from .utils import content_hash, parse_datetime, split_words


class XAdapter(BaseSourceAdapter):
    def __init__(self, config: SourceAdapterConfig, raw_fetcher=None) -> None:
        if config.source != LeadSource.X:
            raise ValueError("XAdapter requires the x source")
        super().__init__(config=config, raw_fetcher=raw_fetcher)

    def normalize_parts(self, raw_item: dict[str, Any]) -> NormalizedLeadParts:
        post_id = str(raw_item.get("id") or raw_item.get("id_str") or "x-post")
        title = str(raw_item.get("text") or raw_item.get("title") or "")
        body = str(raw_item.get("text") or raw_item.get("full_text") or "")
        url = str(raw_item.get("url") or raw_item.get("link") or "")
        author = str(raw_item.get("username") or raw_item.get("author") or raw_item.get("user") or "")
        published_at = parse_datetime(raw_item.get("created_at") or raw_item.get("posted_at"))
        keywords = split_words(title + " " + body)
        conversion_signals = [signal for signal in keywords if signal in {"freelance", "contract", "hire", "dm"}]
        urgency_signals = [signal for signal in keywords if signal in {"urgent", "fix", "bug", "help"}]
        return NormalizedLeadParts(
            lead_id=f"x:{post_id}",
            title=title,
            body=body,
            source_item_id=post_id,
            source_url=url,
            fetched_at=datetime.utcnow(),
            published_at=published_at,
            author_handle=author,
            author_profile_url=None,
            keywords=keywords,
            urgency_signals=urgency_signals,
            conversion_signals=conversion_signals,
            canonical_key=post_id,
            content_hash=content_hash(title, body),
            raw_payload=raw_item,
        )
