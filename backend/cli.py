from __future__ import annotations

from backend.app import build_app


def main() -> None:
    app = build_app()
    app.run_once()
    csv_output = app.export_current_review_csv()
    print(csv_output)


if __name__ == "__main__":
    main()
