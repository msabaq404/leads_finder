## Dependencies and Source APIs

The project is intentionally lightweight, but now includes one required dependency for live Gemini enrichment.

### Python dependencies

- `google.genai` via the `google-genai` package

Install with:

```bash
pip install -r requirements.txt
```

If the dependency is missing, startup fails immediately.

### Environment variables

Use `.env.example` as reference:

- `GEMINI_API_KEY`: enables live Gemini enrichment
- `GEMINI_MODEL`: defaults to `gemini-2.0-flash`
- `GEMINI_TIMEOUT_SECONDS`: request timeout for Gemini
- `LEADS_DB_PATH`: SQLite file path for durable local persistence (default: `leads_finder.db`)

### Source API status

- GitHub Issues: implemented with public endpoint; token support is planned for higher rate limits.
- Hacker News: implemented via public Algolia API.
- Reddit: implemented via public JSON search for MVP; OAuth migration recommended for production.
- Dev.to: implemented via public API.
- X: adapter exists but remains disabled by default until authenticated transport is configured.

### Current behavior

- No API key: startup fails immediately.
- API key + dependency installed: live Gemini enrichment for top-ranked leads.
- Invalid `GEMINI_MODEL`: startup fails immediately.
- App persistence defaults to SQLite; lead and run history survive restarts.
- When started via `python -m backend.serve`, the app runs the pipeline immediately and then repeats on the configured interval from `LEADS_RUN_INTERVAL_MINUTES` (default `60`).
