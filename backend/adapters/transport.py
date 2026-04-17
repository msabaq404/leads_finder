from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from typing import Any

from backend.contracts.source_adapter import FetchPageRequest


@dataclass(slots=True)
class HttpResponse:
    items: list[dict[str, Any]]
    next_cursor: str | None = None
    exhausted: bool = False


class TransportError(RuntimeError):
    pass


class HttpTransport:
    def __init__(self, user_agent: str = "LeadsFinder/1.0") -> None:
        self.user_agent = user_agent

    def get_json(self, url: str, params: dict[str, Any] | None = None, timeout: float = 15.0) -> dict[str, Any]:
        query = f"?{urlencode(params)}" if params else ""
        request = Request(f"{url}{query}", headers={"User-Agent": self.user_agent})
        try:
            with urlopen(request, timeout=timeout) as response:
                payload = response.read().decode("utf-8")
                return json.loads(payload)
        except HTTPError as error:
            raise TransportError(f"HTTP {error.code} while fetching {url}") from error
        except URLError as error:
            raise TransportError(f"Network error while fetching {url}: {error.reason}") from error
        except json.JSONDecodeError as error:
            raise TransportError(f"Invalid JSON from {url}") from error


def utc_to_epoch(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp())


def cursor_page(request: FetchPageRequest, default_page_size: int) -> tuple[int, int]:
    page_size = request.page_size or default_page_size
    try:
        page = int(request.cursor) if request.cursor else 1
    except ValueError:
        page = 1
    return page, page_size
