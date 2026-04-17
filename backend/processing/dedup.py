from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Iterable

from backend.contracts.lead_schema import DedupMetadata, LeadRecord


@dataclass(slots=True)
class DedupOutcome:
    canonical_lead: LeadRecord
    merged_leads: list[LeadRecord] = field(default_factory=list)
    duplicate_leads: list[LeadRecord] = field(default_factory=list)


class DedupEngine:
    def __init__(self, fuzzy_threshold: float = 0.88, window_days: int = 7) -> None:
        self.fuzzy_threshold = fuzzy_threshold
        self.window = timedelta(days=window_days)

    def deduplicate(self, leads: Iterable[LeadRecord]) -> list[DedupOutcome]:
        clusters: list[list[LeadRecord]] = []
        for lead in leads:
            matched_cluster = self._find_cluster(clusters, lead)
            if matched_cluster is None:
                clusters.append([lead])
                continue
            matched_cluster.append(lead)

        outcomes: list[DedupOutcome] = []
        for cluster in clusters:
            canonical = max(cluster, key=self._lead_priority)
            duplicates = [lead for lead in cluster if lead is not canonical]
            merged = []
            self._merge_cluster(canonical, duplicates)
            outcomes.append(
                DedupOutcome(
                    canonical_lead=canonical,
                    merged_leads=merged,
                    duplicate_leads=duplicates,
                )
            )
        return outcomes

    def _find_cluster(self, clusters: list[list[LeadRecord]], candidate: LeadRecord) -> list[LeadRecord] | None:
        for cluster in clusters:
            if self._is_duplicate(cluster[0], candidate):
                return cluster
        return None

    def _is_duplicate(self, existing: LeadRecord, candidate: LeadRecord) -> bool:
        if existing.dedup.content_hash == candidate.dedup.content_hash:
            return True
        if existing.dedup.canonical_key == candidate.dedup.canonical_key:
            return True
        if not self._within_window(existing, candidate):
            return False
        return self._similarity(existing, candidate) >= self.fuzzy_threshold

    def _within_window(self, existing: LeadRecord, candidate: LeadRecord) -> bool:
        existing_time = existing.trace.published_at or existing.trace.fetched_at
        candidate_time = candidate.trace.published_at or candidate.trace.fetched_at
        delta = abs(existing_time - candidate_time)
        return delta <= self.window

    def _similarity(self, existing: LeadRecord, candidate: LeadRecord) -> float:
        existing_text = f"{existing.title}\n{existing.body}".lower()
        candidate_text = f"{candidate.title}\n{candidate.body}".lower()
        return SequenceMatcher(None, existing_text, candidate_text).ratio()

    def _lead_priority(self, lead: LeadRecord) -> tuple[int, float, datetime]:
        score = lead.score_total if lead.score_total is not None else 0.0
        mentions = lead.dedup.mention_count
        timestamp = lead.trace.published_at or lead.trace.fetched_at
        return (mentions, score, timestamp)

    def _merge_cluster(self, canonical: LeadRecord, duplicates: list[LeadRecord]) -> None:
        if not duplicates:
            canonical.dedup.duplicate_of = None
            return

        merged_ids = [duplicate.lead_id for duplicate in duplicates]
        mention_count = 1
        first_seen_candidates = [canonical.trace.fetched_at]
        last_seen_candidates = [canonical.trace.fetched_at]

        for duplicate in duplicates:
            mention_count += duplicate.dedup.mention_count
            merged_ids.extend(duplicate.dedup.merged_from_ids)
            first_seen_candidates.append(duplicate.dedup.first_seen_at or duplicate.trace.fetched_at)
            last_seen_candidates.append(duplicate.dedup.last_seen_at or duplicate.trace.fetched_at)
            if duplicate.rank_reasons:
                for reason in duplicate.rank_reasons:
                    if reason not in canonical.rank_reasons:
                        canonical.rank_reasons.append(reason)

        canonical.dedup.merged_from_ids = merged_ids
        canonical.dedup.mention_count = mention_count
        canonical.dedup.first_seen_at = min(first_seen_candidates)
        canonical.dedup.last_seen_at = max(last_seen_candidates)
        canonical.dedup.duplicate_of = None
        for duplicate in duplicates:
            duplicate.dedup.duplicate_of = canonical.lead_id
            duplicate.dedup.first_seen_at = canonical.dedup.first_seen_at
            duplicate.dedup.last_seen_at = canonical.dedup.last_seen_at
