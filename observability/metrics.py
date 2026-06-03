"""
Run-level metrics: API call counts and timing.

Usage in any script:

    from observability.metrics import RunMetrics

    if __name__ == '__main__':
        with RunMetrics("ingest_daily_prices"):
            main()

On exit (success or failure), writes a JSON snapshot to R2 at:

    metrics/api_calls/{YYYY-MM-DD}/{run_name}_{epoch_ms}.json

The snapshot includes per-function call counts (sourced from
ingestion.utils.av_client._CALL_COUNTS), error counts, duration,
exit status, and run metadata (GitHub Actions run id if present).

Failures inside the metrics writer never propagate — observability
should not break the pipeline.
"""

from __future__ import annotations

import json
import logging
import os
import time
import traceback
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class RunMetrics:
    def __init__(self, run_name: str):
        self.run_name = run_name
        self.start_ts: float | None = None
        self.end_ts: float | None = None

    def __enter__(self):
        self.start_ts = time.time()
        logger.info(f"[metrics] Starting run: {self.run_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_ts = time.time()
        try:
            self._flush(exc_type, exc_val)
        except Exception as e:  # never let observability break the pipeline
            logger.warning(f"[metrics] Failed to write run metrics: {e}")
            traceback.print_exc()
        return False  # don't suppress exceptions

    def _flush(self, exc_type, exc_val):
        # Lazy imports so this module loads even outside the pipeline env
        from ingestion.utils import av_client
        from ingestion.utils import r2_client

        stats = av_client.get_call_stats()
        duration_s = (self.end_ts or time.time()) - (self.start_ts or time.time())

        payload = {
            "run_name": self.run_name,
            "start_utc": _iso(self.start_ts),
            "end_utc": _iso(self.end_ts),
            "duration_seconds": round(duration_s, 2),
            "status": "success" if exc_type is None else "failure",
            "error_type": exc_type.__name__ if exc_type else None,
            "error_message": str(exc_val) if exc_val else None,
            "api_calls": stats,
            "github": _github_context(),
        }

        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        epoch_ms = int(time.time() * 1000)
        key = f"metrics/api_calls/{date_str}/{self.run_name}_{epoch_ms}.json"

        r2_client.upload_json(payload, key)
        logger.info(
            f"[metrics] Wrote run metrics to {key} — "
            f"{stats['total_calls']} API calls, "
            f"{stats['total_errors']} errors, "
            f"{duration_s:.1f}s"
        )


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _github_context() -> dict:
    """Capture useful GitHub Actions env vars. All None when run locally."""
    repo = os.environ.get("GITHUB_REPOSITORY")
    run_id = os.environ.get("GITHUB_RUN_ID")
    run_url = (
        f"https://github.com/{repo}/actions/runs/{run_id}"
        if repo and run_id else None
    )
    return {
        "run_id": run_id,
        "run_number": os.environ.get("GITHUB_RUN_NUMBER"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "run_url": run_url,
        "workflow": os.environ.get("GITHUB_WORKFLOW"),
        "job": os.environ.get("GITHUB_JOB"),
        "event_name": os.environ.get("GITHUB_EVENT_NAME"),  # 'schedule' vs 'workflow_dispatch'
        "actor": os.environ.get("GITHUB_ACTOR"),
        "ref": os.environ.get("GITHUB_REF"),
        "ref_name": os.environ.get("GITHUB_REF_NAME"),
        "sha": os.environ.get("GITHUB_SHA"),
        "repository": repo,
    }
