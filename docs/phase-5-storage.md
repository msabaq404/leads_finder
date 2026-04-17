## Phase 5 Storage Foundation

This phase adds a persistence boundary so pipeline runs and leads can be reviewed later.

### Implemented

- `backend/storage/repository.py` defines the repository contract and an in-memory implementation
- `backend/storage/service.py` persists pipeline runs and lead records

### Notes

- The repository is intentionally in-memory for now.
- The service persists both ranked leads and enriched leads.
- This layer can later be replaced with PostgreSQL without changing the pipeline entry point.

### Next Step

Build a review UI or API layer on top of the repository so ranked leads can be inspected, filtered, and exported.
