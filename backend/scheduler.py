from __future__ import annotations

import os
import time

from backend.app import LeadsFinderApp, build_app


def run_once(app: LeadsFinderApp | None = None):
    app_instance = app or build_app()
    return app_instance.run_once()


def run_scheduler_loop(interval_minutes: int) -> None:
    interval_seconds = max(int(interval_minutes * 60), 60)
    while True:
        run_once()
        time.sleep(interval_seconds)


def main() -> None:
    run_once_only = os.getenv("LEADS_SCHEDULER_RUN_ONCE", "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if run_once_only:
        run_once()
        return

    interval_minutes = int(os.getenv("LEADS_RUN_INTERVAL_MINUTES", "60"))
    run_scheduler_loop(interval_minutes)


if __name__ == "__main__":
    main()
