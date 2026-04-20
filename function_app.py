from __future__ import annotations

import logging

import azure.functions as func

from backend.scheduler import run_once

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.timer_trigger(
    schedule="0 0 * * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True,
)
def run_scheduler_every_60_minutes(timer: func.TimerRequest) -> None:
    try:
        if timer.past_due:
            logging.warning("Timer is past due")

        summary, persisted = run_once()

        logging.info(
            "Scheduler run completed: run_id=%s stored_leads=%s filtered_in=%s filtered_out=%s ranked=%s enriched=%s",
            persisted.run_id,
            persisted.stored_leads,
            summary.filtered_in,
            summary.filtered_out,
            len(summary.ranked),
            len(summary.enriched),
        )

    except Exception:
        logging.exception("Scheduler failed")
        raise