from __future__ import annotations

from typing import Any
from datetime import datetime
import os

from backend.contracts.enums import LeadSource
from backend.contracts.source_adapter import FetchPageRequest, SourceAdapterConfig, RateLimitConfig

from .base import BaseSourceAdapter, NormalizedLeadParts
from .transport import HttpTransport, TransportError
from .utils import content_hash, parse_datetime, split_words


class XAdapter(BaseSourceAdapter):
    def __init__(self, config: SourceAdapterConfig, raw_fetcher=None) -> None:
        if config.source != LeadSource.X:
            raise ValueError("XAdapter requires the x source")
        # Override rate limit: 1 request/second = 60 requests/minute
        config.rate_limit = RateLimitConfig(requests_per_minute=60, burst_limit=1)
        if raw_fetcher is None:
            raw_fetcher = self._build_default_fetcher(config)
        super().__init__(config=config, raw_fetcher=raw_fetcher)

    def _build_default_fetcher(self, config: SourceAdapterConfig):
        api_key = os.getenv("RAPIDAPI_TWITTER_KEY", "").strip()
        api_host = os.getenv("RAPIDAPI_TWITTER_HOST", "twitter241.p.rapidapi.com").strip()
        
        if not api_key:
            raise ValueError("RAPIDAPI_TWITTER_KEY environment variable is required for X adapter")
        
        transport = HttpTransport(
            default_headers={
                "x-rapidapi-key": api_key,
                "x-rapidapi-host": api_host,
            }
        )

        def fetcher(request: FetchPageRequest) -> dict[str, Any]:
            # Focus X search on programming work opportunities and requests.
            search_query = (
                '(freelance OR contractor OR hiring OR "for hire" OR "need developer" OR '
                '"looking for developer" OR "need help") '
                '(python OR javascript OR typescript OR react OR node OR django OR fastapi '
                'OR backend OR frontend OR api OR bug) '
                '-is:retweet -is:reply lang:en'
            )
            
            params = {
                "query": search_query,
                "type": "Latest",
                "count": config.page_size,
            }
            
            # Add cursor if pagination is active
            if request.cursor:
                params["cursor"] = request.cursor
            
            try:
                # Call Twitter API via RapidAPI
                payload = transport.get_json(
                    "https://twitter241.p.rapidapi.com/search-v3",
                    params=params,
                    timeout=config.timeout_seconds,
                )
            except Exception as e:
                raise TransportError(f"Failed to fetch tweets from Twitter API: {str(e)}")
            
            # Strict parsing for twitter241 search-v3 payload only.
            items = [item for item in _extract_tweets_from_search_v3(payload) if _is_programming_work_lead(item)]
            next_cursor = _extract_bottom_cursor_from_search_v3(payload)
            exhausted = next_cursor is None
            
            return {
                "items": items,
                "next_cursor": next_cursor,
                "exhausted": exhausted,
            }

        return fetcher

    def normalize_parts(self, raw_item: dict[str, Any]) -> NormalizedLeadParts:
        post_id = str(raw_item.get("id") or raw_item.get("id_str") or raw_item.get("rest_id") or "x-post")
        title = str(raw_item.get("text") or raw_item.get("title") or "")
        body = str(raw_item.get("text") or raw_item.get("full_text") or "")
        url = str(raw_item.get("url") or raw_item.get("link") or f"https://twitter.com/i/web/status/{post_id}")
        author = str(raw_item.get("username") or raw_item.get("author") or raw_item.get("user") or "unknown")
        published_at = parse_datetime(raw_item.get("created_at") or raw_item.get("posted_at"))
        
        keywords = split_words(title + " " + body)
        conversion_signals = [signal for signal in keywords if signal in {"freelance", "contract", "hire", "dm", "help"}]
        urgency_signals = [signal for signal in keywords if signal in {"urgent", "fix", "bug", "help", "looking"}]
        
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


def _extract_tweets_from_search_v3(payload: dict[str, Any]) -> list[dict[str, Any]]:
    instructions = payload["result"]["timeline_response"]["timeline"]["instructions"]
    items: list[dict[str, Any]] = []

    for instruction in instructions:
        if instruction.get("type") != "TimelineAddEntries" and instruction.get("__typename") != "TimelineAddEntries":
            continue
        for entry in instruction.get("entries", []):
            result = (
                entry.get("content", {})
                .get("content", {})
                .get("tweet_results", {})
                .get("result", {})
            )
            if result.get("__typename") != "Tweet":
                continue

            rest_id = str(result.get("rest_id") or "").strip()
            if not rest_id:
                continue

            text = str(result.get("details", {}).get("full_text") or "")
            created_at = _to_iso_from_created_at_ms(result.get("details", {}).get("created_at_ms"))
            username = str(
                result.get("core", {})
                .get("user_results", {})
                .get("result", {})
                .get("core", {})
                .get("screen_name")
                or "unknown"
            )

            items.append(
                {
                    "id": rest_id,
                    "id_str": rest_id,
                    "rest_id": rest_id,
                    "text": text,
                    "full_text": text,
                    "created_at": created_at,
                    "username": username,
                    "author": username,
                    "user": username,
                    "url": f"https://twitter.com/i/web/status/{rest_id}",
                }
            )

    return items


def _extract_bottom_cursor_from_search_v3(payload: dict[str, Any]) -> str | None:
    cursor = payload.get("cursor", {}).get("bottom")
    if isinstance(cursor, str) and cursor:
        return cursor
    return None


def _to_iso_from_created_at_ms(value: Any) -> str | None:
    if value is None:
        return None
    timestamp_ms = int(value)
    return datetime.utcfromtimestamp(timestamp_ms / 1000.0).isoformat()


def _is_programming_work_lead(item: dict[str, Any]) -> bool:
    text = str(item.get("text") or item.get("full_text") or "").lower()
    if not text:
        return False

    spam_tokens = {
        "assignment",
        "essay",
        "pay someone",
        "nursing",
        "summer class",
        "homework",
    }
    if any(token in text for token in spam_tokens):
        return False

    intent_tokens = {
        "hire",
        "hiring",
        "freelance",
        "contract",
        "contractor",
        "for hire",
        "need dev",
        "need developer",
        "looking for developer",
        "need help",
    }
    tech_tokens = {
        "python",
        "javascript",
        "typescript",
        "react",
        "node",
        "django",
        "fastapi",
        "backend",
        "frontend",
        "api",
        "bug",
        "debug",
        "fix",
    }

    has_intent = any(token in text for token in intent_tokens)
    has_tech = any(token in text for token in tech_tokens)
    return has_intent and has_tech
