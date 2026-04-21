from __future__ import annotations

import json
from dataclasses import asdict
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from backend.app import LeadsFinderApp, build_app
from backend.review.export import export_leads_to_csv
from backend.scheduler import run_once as run_scheduler_once


class LeadsFinderRequestHandler(BaseHTTPRequestHandler):
    app: LeadsFinderApp
    scheduler_interval_minutes: int = 0
    scheduler_enabled: bool = False
    web_root = Path(__file__).resolve().parents[1] / "web"
    asset_root = web_root / "assets"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/":
            self._send_dashboard()
            return

        if path.startswith("/assets/"):
            self._send_asset(path)
            return

        if path == "/health":
            self._send_json(
                {
                    "status": "ok",
                    "scheduler_enabled": self.scheduler_enabled,
                    "scheduler_interval_minutes": self.scheduler_interval_minutes,
                }
            )
            return

        if path == "/api/leads":
            query_params = parse_qs(parsed.query)
            search = (query_params.get("search", [""])[0] or "").strip()
            try:
                page = int((query_params.get("page", ["1"])[0] or "1").strip())
            except ValueError:
                page = 1
            try:
                page_size = int((query_params.get("page_size", ["50"])[0] or "50").strip())
            except ValueError:
                page_size = 50

            safe_page = max(page, 1)
            safe_page_size = min(max(page_size, 1), 200)

            items, total = self.app.review_service.search_review_items(
                search,
                page=safe_page,
                page_size=safe_page_size,
            )
            payload_items = [asdict(item) for item in items]
            self._send_json(
                {
                    "items": payload_items,
                    "count": len(payload_items),
                    "total": total,
                    "page": safe_page,
                    "page_size": safe_page_size,
                    "has_next": (safe_page * safe_page_size) < total,
                }
            )
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

        summary, persisted = run_scheduler_once(self.app)
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
            "skipped_existing": summary.skipped_existing,
            "filtered_out": summary.filtered_out,
            "filtered_in": summary.filtered_in,
            "top_rejection_reasons": list(summary.top_rejection_reasons),
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

    def _send_asset(self, path: str) -> None:
        relative = path.replace("/assets/", "", 1)
        candidate = (self.asset_root / relative).resolve()
        asset_root = self.asset_root.resolve()
        if not str(candidate).startswith(str(asset_root)):
            self._send_json({"error": "invalid_asset_path"}, status=HTTPStatus.BAD_REQUEST)
            return
        if not candidate.exists() or not candidate.is_file():
            self._send_json({"error": "asset_not_found"}, status=HTTPStatus.NOT_FOUND)
            return

        content_type = "application/octet-stream"
        if candidate.suffix == ".css":
            content_type = "text/css; charset=utf-8"
        elif candidate.suffix == ".js":
            content_type = "application/javascript; charset=utf-8"
        elif candidate.suffix == ".json":
            content_type = "application/json; charset=utf-8"
        elif candidate.suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp"}:
            content_type = f"image/{candidate.suffix.lstrip('.') if candidate.suffix != '.jpg' else 'jpeg'}"

        body = candidate.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class LeadsFinderApiServer:
    def __init__(
        self,
        app: LeadsFinderApp | None = None,
        host: str = "0.0.0.0",
        port: int | None = None,
    ) -> None:
        self.app = app or build_app()
        self.host = host
        self.port = self._resolve_port(port)
        self._server = ThreadingHTTPServer((host, self.port), LeadsFinderRequestHandler)
        self._server.RequestHandlerClass.app = self.app
        # Scheduler is decoupled from the web server and should run externally.
        self._server.RequestHandlerClass.scheduler_interval_minutes = 0
        self._server.RequestHandlerClass.scheduler_enabled = False

    def _resolve_port(self, port: int | None) -> int:
        if port is not None:
            return port

        env_port = os.getenv("PORT") or os.getenv("WEBSITES_PORT") or os.getenv("LEADS_PORT")
        if env_port:
            return int(env_port)
            

        return 8000

    def serve_forever(self) -> None:
        self._server.serve_forever()

    def shutdown(self) -> None:
        self._server.shutdown()
        self._server.server_close()
