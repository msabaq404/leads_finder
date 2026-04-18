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
    "azure",
    "astro",
    "svelte"
}

REJECTION_TERMS = {
    "upvote",
    "subscribe",
    "newsletter",
    "giveaway",
    "meme",
    "shitpost",
    "karma",
    "promo code",
    "follow me",
    "like and share",
    "discord invite",
    "telegram channel",
    "course sale",
    "bootcamp discount",
    "open to work",
    "looking for work",
    "seeking opportunities",
    "seeking work",
    "available for work",
    "job seeker",
    "job search",
    "anyone hiring",
    "is anyone hiring",
    "onsite",
    "on-site",
    "in office",
    "in-office",
    "office-based",
}

BLOG_STYLE_TERMS = {
    "how i built",
    "i built",
    "mastering",
    "tutorial",
    "walkthrough",
    "lessons learned",
    "case study",
    "my setup",
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
        rejection_terms: Iterable[str] = REJECTION_TERMS,
        blog_style_terms: Iterable[str] = BLOG_STYLE_TERMS,
        min_confidence: float = 0.15,
    ) -> None:
        self.high_terms = tuple(term.lower() for term in high_terms)
        self.medium_terms = tuple(term.lower() for term in medium_terms)
        self.tech_terms = tuple(term.lower() for term in tech_terms)
        self.rejection_terms = tuple(term.lower() for term in rejection_terms)
        self.blog_style_terms = tuple(term.lower() for term in blog_style_terms)
        self.min_confidence = min_confidence

    def evaluate(self, lead: LeadRecord) -> FilterDecision:
        text = f"{lead.title}\n{lead.body}".lower()
        reasons: list[str] = []
        confidence = 0.0

        candidate_seeking_work = self._has_candidate_seeking_work(text)
        if candidate_seeking_work:
            return FilterDecision(
                accepted=False,
                confidence=0.0,
                reasons=["rejected as job-seeker content"],
            )

        rejection_hits = self._match_terms(text, self.rejection_terms)
        if rejection_hits:
            return FilterDecision(
                accepted=False,
                confidence=0.0,
                reasons=[f"rejected as non-lead content: {', '.join(rejection_hits[:4])}"],
            )

        request_language = self._has_task_request_language(text)
        blog_hits = self._match_terms(text, self.blog_style_terms)
        if blog_hits:
            reasons.append(f"blog/tutorial pattern detected: {', '.join(blog_hits[:3])}")

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

        has_code_like = self._has_code_like_content(text)
        if has_code_like:
            confidence += 0.15
            reasons.append("contains code-like content")

        entity_intent = self._has_entity_task_intent(lead)
        entity_technical = self._has_entity_technical_context(lead)
        non_tech_client_context = self._has_non_technical_client_context(text)
        if entity_intent:
            confidence += 0.1
            reasons.append("structured intent signals from source")
        if entity_technical:
            confidence += 0.05
            reasons.append("structured technical signals from source")
        if non_tech_client_context:
            confidence += 0.1
            reasons.append("non-technical client context detected")

        # has_task_intent = bool(high_hits or medium_hits or request_language or entity_intent)
        has_technical_context = bool(
            tech_hits
            or has_code_like
            or self._has_technical_context_language(text)
            or entity_technical
            or non_tech_client_context
        )

        word_count = len(text.split())
        if word_count >= 25:
            confidence += 0.05
            reasons.append("enough detail for actionable lead")

        if word_count < 5:
            return FilterDecision(
                accepted=False,
                confidence=round(min(confidence, 1.0), 3),
                reasons=reasons + ["too short to classify"],
            )

        accepted = bool(
            request_language
            or entity_intent
            or has_technical_context
            or confidence >= 0.1
        )
        if not accepted and not reasons:
            reasons.append("insufficient pre-filter confidence")
        return FilterDecision(accepted=accepted, confidence=round(min(confidence, 1.0), 3), reasons=reasons)

    def _match_terms(self, text: str, terms: tuple[str, ...]) -> list[str]:
        hits = []
        for term in terms:
            pattern = r"\b" + re.escape(term) + r"\b"
            if re.search(pattern, text):
                hits.append(term)
        return hits

    def _has_code_like_content(self, text: str) -> bool:
        return bool(
            re.search(r"```.+?```", text, re.DOTALL)
            or re.search(r"\b(def|class|function|const|let|var|public|private|import)\b", text)
            or re.search(r"\b(traceback|stack trace|nullpointer|exception|syntaxerror)\b", text)
        )

    def _has_task_request_language(self, text: str) -> bool:
        return bool(
            re.search(r"\b(can someone help|anyone who can|need someone|need a developer|looking for (a )?(developer|engineer)|help with)\b", text)
        )

    def _has_technical_context_language(self, text: str) -> bool:
        return bool(
            re.search(r"\b(api|backend|frontend|fullstack|script|automation|database|server|deployment|integration|scraper|bot)\b", text)
        )

    def _has_entity_task_intent(self, lead: LeadRecord) -> bool:
        intent_tokens = {
            "hiring",
            "hire",
            "hire",
            "freelance",
            "contract",
            "bounty",
            "help wanted",
            "need help",
            "looking for",
            "need someone",
        }
        signals = {value.strip().lower() for value in lead.entities.conversion_signals}
        return bool(signals & intent_tokens)

    def _has_entity_technical_context(self, lead: LeadRecord) -> bool:
        return bool(lead.entities.languages or lead.entities.frameworks or lead.entities.keywords)

    def _has_non_technical_client_context(self, text: str) -> bool:
        client_need = re.search(
            r"\b(need someone|looking for someone|can someone help|want someone|need help|hire someone|hiring someone)\b",
            text,
        )
        business_problem = re.search(
            r"\b(my website|our website|my app|our app|online store|shop|booking|payments?|checkout|invoices?|leads?|customers?|orders?|business|workflow|dashboard|crm|form|landing page|site)\b",
            text,
        )
        pain_signal = re.search(
            r"\b(not working|broken|slow|keep failing|fails|can't|cannot|stuck|issue|problem|need to build|need to improve|set up|setup|integrate|automate)\b",
            text,
        )
        return bool(client_need and (business_problem or pain_signal))

    def _has_candidate_seeking_work(self, text: str) -> bool:
        candidate_signals = re.search(
            r"\b(open to work|looking for work|seeking opportunities|seeking work|available for work|job seeker|job search|resume|portfolio|anyone hiring|is anyone hiring)\b",
            text,
        )
        onsite_signals = re.search(r"\b(onsite|on-site|in office|in-office|office-based)\b", text)
        return bool(candidate_signals or onsite_signals)
