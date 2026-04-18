from __future__ import annotations

import os

from backend.api import LeadsFinderApiServer


def main() -> None:
    schedule_interval_minutes = int(os.getenv("LEADS_RUN_INTERVAL_MINUTES", "60"))
    server = LeadsFinderApiServer(schedule_interval_minutes=schedule_interval_minutes)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
