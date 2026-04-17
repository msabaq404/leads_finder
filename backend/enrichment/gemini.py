from __future__ import annotations

from dataclasses import dataclass
import json
import os
import re
from typing import Any, Protocol


@dataclass(slots=True)
class GeminiBudget:
    daily_request_limit: int = 200
    requests_used: int = 0
    token_limit_per_request: int = 15_000

    def can_request(self) -> bool:
        return self.requests_used < self.daily_request_limit

    def record_request(self) -> None:
        self.requests_used += 1


class GeminiEnricher(Protocol):
    def enrich(self, prompt: str) -> dict[str, Any]:
        """Return a structured enrichment response."""


@dataclass(slots=True)
class GeminiClient:
    model_name: str = "gemini-2.0-flash"
    timeout_seconds: float = 30.0
    api_key: str | None = None

    @classmethod
    def from_env(cls, model_name: str = "gemini-2.0-flash", timeout_seconds: float = 30.0) -> GeminiClient | None:
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            return None
        return cls(model_name=model_name, timeout_seconds=timeout_seconds, api_key=api_key)

    def enrich(self, prompt: str) -> dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("GEMINI_API_KEY is not configured")

        try:
            import google.generativeai as genai
        except ModuleNotFoundError as error:
            raise RuntimeError(
                "google-generativeai is not installed. Install dependencies in requirements.txt."
            ) from error

        genai.configure(api_key=self.api_key)
        model = genai.GenerativeModel(self.model_name)
        response = model.generate_content(prompt)
        text = getattr(response, "text", "") or ""
        return _parse_response_json(text)


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
