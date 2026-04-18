from __future__ import annotations

from dataclasses import dataclass, field
import importlib
import json
import os
import re
from datetime import datetime, timedelta
import time
from typing import Any, Protocol


@dataclass(slots=True)
class GeminiBudget:
    daily_request_limit: int = 20
    requests_per_minute_limit: int = 5
    requests_used: int = 0
    token_limit_per_request: int = 15_000
    request_timestamps: list[datetime] = field(default_factory=list)

    def can_request(self) -> bool:
        if self.is_daily_exhausted():
            return False
        self._prune_old_requests()
        return len(self.request_timestamps) < self.requests_per_minute_limit

    def is_daily_exhausted(self) -> bool:
        return self.requests_used >= self.daily_request_limit

    def seconds_until_request_available(self) -> float:
        self._prune_old_requests()
        if len(self.request_timestamps) < self.requests_per_minute_limit:
            return 0.0
        oldest = min(self.request_timestamps)
        delta = (oldest + timedelta(minutes=1) - datetime.utcnow()).total_seconds()
        return max(delta, 0.0)

    def wait_for_slot(self, max_wait_seconds: float, poll_interval_seconds: float = 0.25) -> bool:
        if max_wait_seconds < 0:
            max_wait_seconds = 0.0
        poll = max(poll_interval_seconds, 0.01)
        deadline = time.monotonic() + max_wait_seconds

        while True:
            if self.can_request():
                return True
            if self.is_daily_exhausted():
                return False

            remaining_wait = deadline - time.monotonic()
            if remaining_wait <= 0:
                return False

            rate_wait = self.seconds_until_request_available()
            sleep_for = min(remaining_wait, max(rate_wait, poll))
            time.sleep(max(sleep_for, 0.01))

    def record_request(self) -> None:
        self._prune_old_requests()
        self.requests_used += 1
        self.request_timestamps.append(datetime.utcnow())

    def _prune_old_requests(self) -> None:
        cutoff = datetime.utcnow() - timedelta(minutes=1)
        self.request_timestamps = [value for value in self.request_timestamps if value >= cutoff]


class GeminiEnricher(Protocol):
    def enrich(self, prompt: str) -> dict[str, Any]:
        """Return a structured enrichment response."""


@dataclass(slots=True)
class GeminiClient:
    model_name: str = "gemini-2.0-flash"
    timeout_seconds: float = 30.0
    api_key: str | None = None
    _resolved_model_name: str | None = None

    @classmethod
    def from_env(cls, model_name: str = "gemini-2.0-flash", timeout_seconds: float = 30.0) -> GeminiClient:
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY is not configured")
        return cls(model_name=model_name, timeout_seconds=timeout_seconds, api_key=api_key)

    def enrich(self, prompt: str) -> dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("GEMINI_API_KEY is not configured")

        genai = _import_google_genai()

        client = genai.Client(api_key=self.api_key)
        response = client.models.generate_content(model=self.model_name, contents=prompt)
        text = str(getattr(response, "text", "") or "")
        return _parse_response_json(text)

    def list_models(self) -> list[str]:
        if not self.api_key:
            raise RuntimeError("GEMINI_API_KEY is not configured")

        genai = _import_google_genai()

        client = genai.Client(api_key=self.api_key)
        return self._list_generate_models(client)

    def _list_generate_models(self, client: Any) -> list[str]:
        names: list[str] = []
        for model in client.models.list():
            methods = getattr(model, "supported_actions", None) or getattr(model, "supported_generation_methods", None) or []
            normalized_methods = {str(method) for method in methods}
            if "generateContent" in normalized_methods or "models.generate_content" in normalized_methods or not normalized_methods:
                name = str(getattr(model, "name", "") or "").strip()
                if name:
                    names.append(name)
        return names


def _import_google_genai():
    try:
        return importlib.import_module("google.genai")
    except (ModuleNotFoundError, ImportError) as error:
        raise RuntimeError(
            "google-genai is not installed. Install dependencies in requirements.txt."
        ) from error


def _parse_response_json(text: str) -> dict[str, Any]:
    if not text.strip():
        return {}

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    block_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if block_match:
        try:
            parsed = json.loads(block_match.group(1))
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

    object_match = re.search(r"\{.*\}", text, re.DOTALL)
    if object_match:
        try:
            parsed = json.loads(object_match.group(0))
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

    return {}
