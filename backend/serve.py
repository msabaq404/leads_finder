from __future__ import annotations

from backend.api import LeadsFinderApiServer


def main() -> None:
    server = LeadsFinderApiServer(host="0.0.0.0")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
