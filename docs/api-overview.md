## API Overview

The backend now exposes a small stdlib HTTP API.

### Endpoints

- `GET /` - dashboard UI
- `GET /health` - health check
- `GET /api/leads` - list ranked review items
- `GET /api/runs` - list persisted pipeline runs
- `GET /api/export.csv` - download the current review export
- `POST /api/run` - run ingestion, filtering, deduplication, ranking, and enrichment once

### Notes

This API is intentionally minimal and dependency-free. It can later be replaced or wrapped by a higher-level framework if needed.
