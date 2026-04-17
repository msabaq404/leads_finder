## Phase 2 Ingestion Foundation

This phase adds concrete source adapters and an adapter registry that can be wired into workers later.

### Implemented

- `backend/adapters/base.py` shared adapter behavior
- `backend/adapters/utils.py` stable normalization helpers
- `backend/adapters/github_issues.py`
- `backend/adapters/hacker_news.py`
- `backend/adapters/reddit.py`
- `backend/adapters/dev_to.py`
- `backend/adapters/x.py`
- `backend/ingestion/registry.py` with disabled-by-default default registry

### Notes

- Each adapter validates it is configured for the correct source.
- Each adapter exposes health tracking through the shared base.
- Content hashes are now stable across runs using SHA-256.
- The default registry disables external calls until real transport credentials are added.

### Next Step

Wire a worker that iterates enabled adapters, fetches a page per source, normalizes items, and persists normalized leads.