from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Iterable

from backend.contracts.lead_schema import LeadRecord

HIGH_CONFIDENCE_TERMS = {
    "bug",
    "debug",
    "error",
    "exception",
    "stack trace",
    "freelance",
    "contract",
    "hiring",
    "help wanted",
    "api integration",
    "refactor",
    "performance",
    "optimization",
    "security",
}

MEDIUM_CONFIDENCE_TERMS = {
    "fix",
    "issue",
    "build",
    "implement",
    "looking for",
    "need help",
    "remote",
    "part time",
    "consultant",
}

TECH_TERMS = {
    "python",
    "javascript",
    "typescript",
    "react",
    "node",
    "django",
    "fastapi",
    "flask",
    "vue",
    "angular",
    "postgres",
    "mysql",
    "redis",
    "docker",
    "kubernetes",
    "aws",
}


@dataclass(slots=True)
class FilterDecision:
    accepted: bool
    confidence: float
    reasons: list[str] = field(default_factory=list)


class ProgrammingTaskFilter:
    def __init__(
        self,
        high_terms: Iterable[str] = HIGH_CONFIDENCE_TERMS,
        medium_terms: Iterable[str] = MEDIUM_CONFIDENCE_TERMS,
        tech_terms: Iterable[str] = TECH_TERMS,
        min_confidence: float = 0.45,
    ) -> None:
        self.high_terms = tuple(term.lower() for term in high_terms)
        self.medium_terms = tuple(term.lower() for term in medium_terms)
        self.tech_terms = tuple(term.lower() for term in tech_terms)
        self.min_confidence = min_confidence

    def evaluate(self, lead: LeadRecord) -> FilterDecision:
        text = f"{lead.title}\n{lead.body}".lower()
        reasons: list[str] = []
        confidence = 0.0

        high_hits = self._match_terms(text, self.high_terms)
        if high_hits:
            confidence += 0.45
            reasons.append(f"high-confidence signals: {', '.join(high_hits[:4])}")

        medium_hits = self._match_terms(text, self.medium_terms)
        if medium_hits:
            confidence += 0.2
            reasons.append(f"medium-confidence signals: {', '.join(medium_hits[:4])}")

        tech_hits = self._match_terms(text, self.tech_terms)
        if tech_hits:
            confidence += 0.15
            reasons.append(f"tech stack mentions: {', '.join(tech_hits[:4])}")

        if self._has_code_like_content(text):
            confidence += 0.15
            reasons.append("contains code-like content")

        if len(text.split()) >= 25:
            confidence += 0.05
            reasons.append("enough detail for actionable lead")

        accepted = confidence >= self.min_confidence
        if not accepted and not reasons:
            reasons.append("insufficient programming-task intent")
        return FilterDecision(accepted=accepted, confidence=round(min(confidence, 1.0), 3), reasons=reasons)

    def _match_terms(self, text: str, terms: tuple[str, ...]) -> list[str]:
        return [term for term in terms if term in text]

    def _has_code_like_content(self, text: str) -> bool:
        return bool(
            re.search(r"```.+?```", text, re.DOTALL)
            or re.search(r"\b(def|class|function|const|let|var|public|private|import)\b", text)
            or re.search(r"\b(traceback|stack trace|nullpointer|exception|syntaxerror)\b", text)
        )
