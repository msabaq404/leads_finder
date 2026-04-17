## Phase 3 Filtering, Deduplication, and Ranking

This phase turns raw normalized leads into ranked, explainable lead candidates.

### Implemented

- `backend/processing/filtering.py` rules-first programming-task intent filter
- `backend/processing/dedup.py` exact and fuzzy deduplication engine
- `backend/processing/ranking.py` balanced ranking engine built on the Phase 1 score contract

### Behavioral Notes

- Filtering uses high-confidence, medium-confidence, and tech-stack signals.
- Deduplication prefers exact content and canonical-key matches, then fuzzy similarity within a time window.
- Ranking produces a deterministic score breakdown and reason list for each lead.

### Intended Flow

1. Normalize leads from sources
2. Filter low-intent entries
3. Deduplicate overlapping records
4. Rank the remaining leads
5. Present the scored results in the review UI

### Next Step

Add persistence and review interfaces once the ranking pipeline is stable enough to store and inspect results.
