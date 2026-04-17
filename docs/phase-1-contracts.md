## Phase 1 Implementation: Product Contracts and Data Schema

This document captures the initial implementation for Phase 1.

### What Was Implemented

1. Canonical lead schema in backend/contracts/lead_schema.py
2. Source adapter contract in backend/contracts/source_adapter.py
3. Balanced ranking contract in backend/contracts/ranking.py
4. Shared enums in backend/contracts/enums.py

### Canonical Lead Schema Coverage

The LeadRecord contract now includes:

- Source metadata (source, source_item_id, source_url, fetched_at, published_at)
- Normalized text fields (title, body)
- Extracted entities (keywords, languages, frameworks, conversion and urgency signals)
- Score breakdown and explainability reasons
- Dedup metadata (canonical key, hash, merge references, mention count)
- Manual review and CSV export state fields
- Raw payload preservation for auditability

### Source Adapter Contract Coverage

The SourceAdapter protocol defines:

- Cursor and window-based page fetching
- Item-to-canonical normalization
- Health reporting for observability/circuit controls

The config classes include:

- Rate-limit policy
- Retry and backoff policy
- Per-source feature flags and timeout/page-size controls

### Ranking Contract Coverage

Balanced objective is formalized via RankingWeights:

- conversion_likelihood
- urgency
- lead_quality

The compute_balanced_score utility validates weight totals and produces a deterministic LeadScoreBreakdown.

### Next Phase Hand-off

Phase 2 can now implement concrete adapters for:

- Reddit
- X
- GitHub Issues
- Hacker News
- Dev.to

All adapters should satisfy the SourceAdapter protocol and emit LeadRecord.
