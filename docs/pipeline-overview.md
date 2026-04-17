## Pipeline Overview

The current implementation now supports an end-to-end flow:

1. Source ingestion via per-source adapters
2. Rules-based filtering for programming-task intent
3. Exact and fuzzy deduplication
4. Balanced ranking with explainable scores
5. Selective Gemini enrichment with cache and fallback

### Entry Point

`backend/pipeline/engine.py` provides the orchestration class `LeadPipeline`.

### Output

A pipeline run returns a summary with:

- Ingestion results per source
- Filter counts
- Dedup group count
- Ranked canonical leads
- Enrichment results for top-ranked items

### Current Status

This is still an in-memory orchestration layer. Persistence and UI are the next implementation targets.
