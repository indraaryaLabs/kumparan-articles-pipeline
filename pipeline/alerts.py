"""Failure notification used as the Airflow on_failure_callback.

Logs the failure, marks the batch as failed in meta.etl_run_log, and posts to
a Slack incoming webhook when SLACK_WEBHOOK_URL is set. Never raises: a broken
alert must not hide the original error.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request

log = logging.getLogger(__name__)


def on_task_failure(context) -> None:
    ti = context.get("ti") or context.get("task_instance")
    dag_id = getattr(ti, "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    run_id = context.get("run_id", "?")
    error = context.get("exception")
    message = f"[{dag_id}] task {task_id} failed in run {run_id}: {type(error).__name__ if error else ''} {error or ''}"
    log.error(message)

    try:
        plan = ti.xcom_pull(task_ids="plan") if ti else None
        batch_id = plan.get("batch_id") if isinstance(plan, dict) else None
        if batch_id:
            from pipeline import runlog
            from pipeline.config import Settings

            runlog.fail(Settings.from_env(), batch_id, message)
    except Exception:  # noqa: BLE001 - best effort only
        log.exception("could not mark the run as failed in meta.etl_run_log")

    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            req = urllib.request.Request(
                webhook, data=json.dumps({"text": message}).encode(), headers={"Content-Type": "application/json"}
            )
            urllib.request.urlopen(req, timeout=10)  # noqa: S310 - URL comes from our own config
        except Exception:  # noqa: BLE001
            log.exception("could not post the failure to Slack")
