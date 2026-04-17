## Testing

The backend now includes a stdlib unittest suite in `backend/tests/test_pipeline_and_api.py`.

### Coverage

- End-to-end pipeline flow with fake ingestion input
- HTTP API health, run, leads, runs, and CSV endpoints
- In-memory repository persistence

### Notes

The tests avoid live network access by using in-memory and fake pipeline objects.
