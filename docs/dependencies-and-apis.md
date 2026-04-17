## Dependencies and Source APIs

The project is intentionally lightweight, but now includes one optional dependency for live Gemini enrichment.

### Python dependencies

- `google-generativeai` (optional at runtime, required only for live Gemini calls)

Install with:

```bash
pip install -r requirements.txt
```

If dependency is missing, enrichment automatically falls back to deterministic local logic.

### Environment variables

Use `.env.example` as reference:

- `GEMINI_API_KEY`: enables live Gemini enrichment
- `GEMINI_MODEL`: defaults to `gemini-2.0-flash`
- `GEMINI_TIMEOUT_SECONDS`: request timeout for Gemini

### Source API status

- GitHub Issues: implemented with public endpoint; token support is planned for higher rate limits.
- Hacker News: implemented via public Algolia API.
- Reddit: implemented via public JSON search for MVP; OAuth migration recommended for production.
- Dev.to: implemented via public API.
- X: adapter exists but remains disabled by default until authenticated transport is configured.

### Current behavior

- No API key: fallback enrichment only.
- API key + dependency installed: live Gemini enrichment for top-ranked leads.
