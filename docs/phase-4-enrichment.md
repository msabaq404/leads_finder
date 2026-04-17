## Phase 4 Gemini Enrichment

This phase adds selective enrichment for only the highest-ranked leads.

### Implemented

- `backend/contracts/lead_schema.py` now includes an `enrichment` field on `LeadRecord`
- `backend/enrichment/cache.py` for TTL-based result reuse
- `backend/enrichment/gemini.py` for budget and transport abstraction
- `backend/enrichment/service.py` for selective enrichment, fallback summaries, and cache integration

### Operating Rules

- Enrichment only processes the top fraction of ranked leads.
- Cache hits skip Gemini calls.
- If Gemini is unavailable or the budget is exhausted, deterministic fallback enrichment is used.
- Response normalization clamps unsafe or malformed data to safe defaults.

### Next Step

Wire enrichment into the ranking pipeline and add persistence for enriched lead records.
