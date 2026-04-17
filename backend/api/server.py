from __future__ import annotations

import json
from dataclasses import asdict
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from backend.app import LeadsFinderApp, build_app
from backend.review.export import export_leads_to_csv


class LeadsFinderRequestHandler(BaseHTTPRequestHandler):
    app: LeadsFinderApp
    web_root = Path(__file__).resolve().parents[1] / "web"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/":
            self._send_dashboard()
            return

        if path == "/health":
            self._send_json({"status": "ok"})
            return

        if path == "/api/leads":
            items = [asdict(item) for item in self.app.list_review_items()]
            self._send_json({"items": items, "count": len(items)})
            return

        if path == "/api/runs":
            runs = [
                {
                    "run_id": run.run_id,
                    "created_at": run.created_at.isoformat(),
                    "summary": self._serialize_pipeline_summary(run.summary),
                }
                for run in self.app.repository.get_pipeline_runs()
            ]
            self._send_json({"items": runs, "count": len(runs)})
            return

        if path == "/api/export.csv":
            csv_text = export_leads_to_csv(self.app.list_review_items())
            self._send_text(csv_text, content_type="text/csv; charset=utf-8")
            return

        self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/api/run":
            self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)
            return

        summary, persisted = self.app.run_once()
        payload = {
            "persisted_run_id": persisted.run_id,
            "stored_leads": persisted.stored_leads,
            "summary": self._serialize_pipeline_summary(summary),
        }
        self._send_json(payload, status=HTTPStatus.ACCEPTED)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _serialize_pipeline_summary(self, summary) -> dict[str, Any]:
        return {
            "ingestion": {
                "started_at": summary.ingestion.started_at.isoformat(),
                "finished_at": summary.ingestion.finished_at.isoformat(),
                "leads_count": len(summary.ingestion.leads),
                "per_source": [asdict(item) for item in summary.ingestion.per_source],
            },
            "filtered_out": summary.filtered_out,
            "filtered_in": summary.filtered_in,
            "deduped_groups": summary.deduped_groups,
            "ranked_count": len(summary.ranked),
            "enriched_count": len(summary.enriched),
        }

    def _send_json(self, data: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, text: str, status: HTTPStatus = HTTPStatus.OK, content_type: str = "text/plain; charset=utf-8") -> None:
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_dashboard(self) -> None:
        index_path = self.web_root / "index.html"
        if not index_path.exists():
            self._send_json({"error": "dashboard_not_found"}, status=HTTPStatus.NOT_FOUND)
            return
        html = index_path.read_text(encoding="utf-8")
        self._send_text(html, content_type="text/html; charset=utf-8")


class LeadsFinderApiServer:
    def __init__(self, app: LeadsFinderApp | None = None, host: str = "127.0.0.1", port: int = 8000) -> None:
        self.app = app or build_app()
        self.host = host
        self.port = port
        self._server = ThreadingHTTPServer((host, port), LeadsFinderRequestHandler)
        self._server.RequestHandlerClass.app = self.app

    def serve_forever(self) -> None:
        self._server.serve_forever()

    def shutdown(self) -> None:
        self._server.shutdown()
        self._server.server_close()
