## Composition Root

The current backend has a single assembly point in `backend/app.py`.

### Components Wired Together

- `build_default_registry()` creates the source adapter registry
- `IngestionWorker` fetches and normalizes leads
- `LeadPipeline` filters, deduplicates, ranks, and enriches
- `InMemoryLeadRepository` stores runs and leads in memory
- `ReviewService` exposes ranked leads for review
- `export_leads_to_csv()` converts review rows to CSV

### Purpose

This keeps the lower-level contracts isolated while still giving the app one top-level path for execution.
