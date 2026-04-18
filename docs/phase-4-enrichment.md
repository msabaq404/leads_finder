## Phase 4 Gemini Enrichment

This phase adds selective enrichment for only the highest-ranked leads.

### Implemented

- `backend/contracts/lead_schema.py` now includes an `enrichment` field on `LeadRecord`
- `backend/enrichment/cache.py` for TTL-based result reuse
- `backend/enrichment/gemini.py` for budget and transport abstraction
- `backend/enrichment/service.py` for selective enrichment, strict validation, and cache integration

### Operating Rules

- Enrichment only processes the top fraction of ranked leads.
- Cache hits skip Gemini calls.
- If Gemini is unavailable, the budget is exhausted, or the model response is malformed, the run fails immediately.
- Response normalization now enforces required fields and raises on invalid data.

### Next Step

Wire enrichment into the ranking pipeline and add persistence for enriched lead records.
