from __future__ import annotations

import csv
import io
from typing import Iterable

from .service import ReviewItem


def export_leads_to_csv(items: Iterable[ReviewItem]) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "lead_id",
        "title",
        "source",
        "score_total",
        "status",
        "summary",
        "reasons",
    ])
    for item in items:
        writer.writerow([
            item.lead_id,
            item.title,
            item.source,
            f"{item.score_total:.6f}",
            item.status,
            item.summary,
            " | ".join(item.reasons),
        ])
    return buffer.getvalue()
