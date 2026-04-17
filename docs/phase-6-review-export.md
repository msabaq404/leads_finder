## Phase 6 Manual Review and CSV Export

This phase exposes ranked leads for manual inspection and offline export.

### Implemented

- `backend/review/service.py` converts repository records into review-friendly rows
- `backend/review/export.py` exports review rows to CSV

### Review Fields

- Lead ID
- Title
- Source
- Score
- Status
- Summary
- Ranking reasons

### Next Step

Replace the current in-memory review path with a web UI or API endpoint once the interaction model is finalized.
