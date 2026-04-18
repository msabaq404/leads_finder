from __future__ import annotations

from datetime import datetime, timezone
from html import unescape
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET
import os
import re

from backend.contracts.enums import LeadSource
from backend.contracts.source_adapter import FetchPageRequest, SourceAdapterConfig

from .base import BaseSourceAdapter, NormalizedLeadParts
from .transport import TransportError, cursor_page
from .utils import content_hash, parse_datetime, parse_unix_datetime, split_words


DEFAULT_REDDIT_RSS_SUBREDDITS = (
    "forhire",
    "freelance_forhire",
    "webdev",
    "programming",
    "Python",
    "javascript",
    "reactjs",
    "node",
    "django",
    "learnprogramming",
)


class RedditAdapter(BaseSourceAdapter):
    """Ingest programming-work leads from official Reddit RSS feeds."""

    def __init__(self, config: SourceAdapterConfig, raw_fetcher=None) -> None:
        if config.source != LeadSource.REDDIT:
            raise ValueError("RedditAdapter requires the reddit source")
        if raw_fetcher is None:
            raw_fetcher = self._build_default_fetcher(config)
        super().__init__(config=config, raw_fetcher=raw_fetcher)

    def _build_default_fetcher(self, config: SourceAdapterConfig):
        subreddits = tuple(
            subreddit.strip().lstrip("r/").strip()
            for subreddit in os.getenv(
                "REDDIT_RSS_SUBREDDITS",
                ",".join(DEFAULT_REDDIT_RSS_SUBREDDITS),
            ).split(",")
            if subreddit.strip()
        )

        def fetcher(request: FetchPageRequest) -> dict[str, Any]:
            page, page_size = cursor_page(request, config.page_size)
            items: list[dict[str, Any]] = []

            for subreddit in subreddits:
                feed_url = f"https://www.reddit.com/r/{subreddit}/new/.rss"
                feed_xml = _fetch_rss(feed_url, timeout=config.timeout_seconds)
                items.extend(_parse_atom_feed(feed_xml, subreddit=subreddit, feed_url=feed_url))

            windowed = [
                item
                for item in items
                if _item_within_window(item, request.from_time, request.to_time)
            ]
            windowed.sort(
                key=lambda item: item.get("published_at") or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )

            start = (page - 1) * page_size
            end = start + page_size
            page_items = windowed[start:end]
            exhausted = end >= len(windowed)
            next_cursor = str(page + 1) if not exhausted else None
            return {"items": page_items, "next_cursor": next_cursor, "exhausted": exhausted}

        return fetcher

    def normalize_parts(self, raw_item: dict[str, Any]) -> NormalizedLeadParts:
        post_id = str(raw_item.get("id") or raw_item.get("name") or raw_item.get("source_item_id") or "reddit-post")
        title = str(raw_item.get("title") or "")
        body = str(raw_item.get("body") or raw_item.get("summary") or raw_item.get("selftext") or "")
        url = str(raw_item.get("url") or raw_item.get("permalink") or raw_item.get("link") or "")
        author = str(raw_item.get("author") or "")
        published_at = parse_datetime(raw_item.get("published") or raw_item.get("updated")) or parse_unix_datetime(raw_item.get("created_utc") or raw_item.get("created_at"))
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


def _fetch_rss(url: str, timeout: float) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": "LeadsFinder/1.0 Reddit RSS",
            "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8")
    except HTTPError as error:
        raise TransportError(f"HTTP {error.code} while fetching {url}") from error
    except URLError as error:
        raise TransportError(f"Network error while fetching {url}: {error.reason}") from error


def _parse_atom_feed(feed_xml: str, subreddit: str, feed_url: str) -> list[dict[str, Any]]:
    atom = "http://www.w3.org/2005/Atom"
    content_ns = "http://purl.org/rss/1.0/modules/content/"
    root = ET.fromstring(feed_xml)
    items: list[dict[str, Any]] = []

    for entry in root.findall(f"{{{atom}}}entry"):
        title = _clean_text(entry.findtext(f"{{{atom}}}title") or "")
        link = _extract_atom_link(entry, atom) or ""
        author = _clean_text(entry.findtext(f"{{{atom}}}author/{{{atom}}}name") or "")
        published_raw = entry.findtext(f"{{{atom}}}published") or entry.findtext(f"{{{atom}}}updated") or ""
        summary = _clean_text(
            entry.findtext(f"{{{atom}}}summary")
            or entry.findtext(f"{{{content_ns}}}encoded")
            or ""
        )
        item_id = _clean_text(entry.findtext(f"{{{atom}}}id") or link or title)
        published_at = parse_datetime(published_raw)
        items.append(
            {
                "id": item_id,
                "name": item_id,
                "title": title,
                "summary": summary,
                "body": summary,
                "url": link,
                "link": link,
                "author": author,
                "published": published_at.isoformat() if published_at else published_raw,
                "updated": published_at.isoformat() if published_at else published_raw,
                "subreddit": subreddit,
                "feed_url": feed_url,
                "published_at": published_at,
            }
        )
    return items


def _extract_atom_link(entry: ET.Element, atom: str) -> str | None:
    for link in entry.findall(f"{{{atom}}}link"):
        rel = (link.attrib.get("rel") or "alternate").lower()
        href = (link.attrib.get("href") or "").strip()
        if rel == "alternate" and href:
            return href
    return None


def _clean_text(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _item_within_window(item: dict[str, Any], from_time: datetime, to_time: datetime) -> bool:
    published_at = item.get("published_at")
    if not isinstance(published_at, datetime):
        return True
    from_time_utc = _ensure_utc(from_time)
    to_time_utc = _ensure_utc(to_time)
    published_at_utc = _ensure_utc(published_at)
    return from_time_utc <= published_at_utc <= to_time_utc


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
