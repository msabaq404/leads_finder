from __future__ import annotations

from datetime import datetime, timedelta
import unittest

from backend.contracts.enums import LeadSource
from backend.contracts.lead_schema import DedupMetadata, ExtractedEntities, LeadRecord, LeadTrace
from backend.processing.filtering import ProgrammingTaskFilter


class ProgrammingTaskFilterTests(unittest.TestCase):
    def test_accepts_actionable_programming_task(self) -> None:
        lead = self._build_lead(
            title="Need help debugging FastAPI integration error",
            body=(
                "Looking for a contractor to fix a Python FastAPI API integration bug. "
                "We can share stack trace details and repository context for a quick turnaround."
            ),
        )

        decision = ProgrammingTaskFilter().evaluate(lead)

        self.assertTrue(decision.accepted)
        self.assertGreaterEqual(decision.confidence, 0.6)

    def test_rejects_promotional_or_garbage_post(self) -> None:
        lead = self._build_lead(
            title="Subscribe for giveaway and promo code",
            body=(
                "Like and share this newsletter for a bootcamp discount. "
                "Join our discord invite and telegram channel now."
            ),
        )

        decision = ProgrammingTaskFilter().evaluate(lead)

        self.assertFalse(decision.accepted)
        self.assertEqual(decision.confidence, 0.0)
        self.assertTrue(any("rejected as non-lead content" in reason for reason in decision.reasons))

    def test_rejects_post_without_clear_task_intent(self) -> None:
        lead = self._build_lead(
            title="Python and React are cool",
            body=(
                "General chat about tech trends, favorite tools, and learning resources. "
                "No concrete task request or project scope in this discussion thread."
            ),
        )

        decision = ProgrammingTaskFilter().evaluate(lead)

        self.assertTrue(decision.accepted)
        self.assertTrue(any("tech stack mentions" in reason or "structured technical" in reason for reason in decision.reasons))

    def test_rejects_blog_style_post_without_help_request(self) -> None:
        lead = self._build_lead(
            title="I built an AI-powered Kali lab",
            body=(
                "I built this setup after a conference demo. This walkthrough explains my setup "
                "and lessons learned from the project."
            ),
        )

        decision = ProgrammingTaskFilter().evaluate(lead)

        self.assertFalse(decision.accepted)
        self.assertTrue(any("blog/tutorial pattern detected" in reason for reason in decision.reasons))

    def test_accepts_blog_style_post_if_help_is_explicit(self) -> None:
        lead = self._build_lead(
            title="I built a scraper and now need help fixing deployment",
            body=(
                "I built a backend scraper and need someone who can help with deployment and API integration. "
                "Looking for a developer to troubleshoot server errors."
            ),
        )

        decision = ProgrammingTaskFilter().evaluate(lead)

        self.assertTrue(decision.accepted)

    def test_accepts_short_post_when_structured_signals_are_strong(self) -> None:
        lead = self._build_lead(
            title="Need FastAPI help",
            body="Hiring backend dev.",
        )
        lead.entities.conversion_signals = ["hiring", "need help"]
        lead.entities.languages = ["python"]
        lead.entities.frameworks = ["fastapi"]
        lead.entities.keywords = ["api", "backend"]

        decision = ProgrammingTaskFilter(min_confidence=0.35).evaluate(lead)

        self.assertTrue(decision.accepted)

    def test_accepts_non_technical_client_request(self) -> None:
        lead = self._build_lead(
            title="Need someone to fix my business website",
            body=(
                "I run a small business and our website checkout is not working. "
                "I am not technical and need someone to help us set it up properly."
            ),
        )

        decision = ProgrammingTaskFilter().evaluate(lead)

        self.assertTrue(decision.accepted)
        self.assertTrue(any("non-technical client context detected" in reason for reason in decision.reasons))

    def test_rejects_job_seeker_profile(self) -> None:
        lead = self._build_lead(
            title="Open to work as a Python developer",
            body=(
                "Looking for opportunities in backend development and remote roles. "
                "Portfolio and resume available on request."
            ),
        )

        decision = ProgrammingTaskFilter().evaluate(lead)

        self.assertFalse(decision.accepted)
        self.assertTrue(any("job-seeker content" in reason for reason in decision.reasons))

    def test_rejects_onsite_job_post(self) -> None:
        lead = self._build_lead(
            title="Hiring a Python engineer for onsite role",
            body=(
                "We need a backend developer for an in-office position at our company. "
                "This onsite role is based in our headquarters."
            ),
        )

        decision = ProgrammingTaskFilter().evaluate(lead)

        self.assertFalse(decision.accepted)
        self.assertTrue(any("job-seeker content" in reason or "rejected as non-lead content" in reason for reason in decision.reasons))

    def _build_lead(self, title: str, body: str) -> LeadRecord:
        now = datetime.utcnow()
        return LeadRecord(
            lead_id="test-lead",
            title=title,
            body=body,
            trace=LeadTrace(
                source=LeadSource.REDDIT,
                source_item_id="src-1",
                source_url="https://example.com/post",
                fetched_at=now,
                published_at=now - timedelta(hours=2),
                author_handle="tester",
            ),
            entities=ExtractedEntities(),
            dedup=DedupMetadata(canonical_key="test", content_hash="hash-test"),
            raw_payload={},
            normalized_at=now,
        )


if __name__ == "__main__":
    unittest.main()
