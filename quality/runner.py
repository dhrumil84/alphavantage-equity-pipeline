"""
Data-quality runner.

Usage:
  python -m quality.runner --suite daily     # post-daily-prices
  python -m quality.runner --suite weekly    # post-weekly-refresh
  python -m quality.runner --suite full      # everything

Behaviour:
  - Loads silver tables via DuckDB (httpfs against R2).
  - Runs the named suite.
  - Writes a JSON report to r2://{bucket}/quality/{YYYY-MM-DD}_{suite}_{epoch}.json
  - Prints a summary table to stdout.
  - Exits 1 if any CRITICAL check failed; 0 otherwise (warns do not fail).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import duckdb
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from ingestion.utils import r2_client  # noqa: E402
from quality import checks as q  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def _github_context() -> dict:
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
        "event_name": os.environ.get("GITHUB_EVENT_NAME"),
        "actor": os.environ.get("GITHUB_ACTOR"),
        "ref_name": os.environ.get("GITHUB_REF_NAME"),
        "sha": os.environ.get("GITHUB_SHA"),
        "repository": repo,
    }


def _duckdb_to_r2() -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection configured to read from Cloudflare R2."""
    account_id = os.environ["R2_ACCOUNT_ID"]
    access_key = os.environ["R2_ACCESS_KEY_ID"]
    secret_key = os.environ["R2_SECRET_ACCESS_KEY"]

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint = '{account_id}.r2.cloudflarestorage.com';")
    con.execute(f"SET s3_access_key_id = '{access_key}';")
    con.execute(f"SET s3_secret_access_key = '{secret_key}';")
    con.execute("SET s3_region = 'auto';")
    con.execute("SET s3_url_style = 'path';")
    return con


def run(suite_name: str) -> int:
    bucket = os.environ["R2_BUCKET_NAME"]
    con = _duckdb_to_r2()

    suite_fn = {
        "daily": q.daily_suite,
        "weekly": q.weekly_suite,
        "full": q.full_suite,
    }[suite_name]

    started = datetime.now(timezone.utc)
    logger.info(f"Running {suite_name} quality suite against bucket={bucket}")

    results: list[q.CheckResult] = []
    for check_fn_result in [suite_fn(con, bucket)]:
        results.extend(check_fn_result)

    finished = datetime.now(timezone.utc)

    summary = {
        "total": len(results),
        "passed": sum(1 for r in results if r.status == "pass"),
        "failed": sum(1 for r in results if r.status == "fail"),
        "critical_failed": sum(1 for r in results if r.status == "fail" and r.severity == "critical"),
        "warn_failed": sum(1 for r in results if r.status == "fail" and r.severity == "warn"),
    }

    report = {
        "suite": suite_name,
        "started_utc": started.isoformat(),
        "finished_utc": finished.isoformat(),
        "duration_seconds": round((finished - started).total_seconds(), 2),
        "bucket": bucket,
        "summary": summary,
        "results": [r.to_dict() for r in results],
        "github": _github_context(),
    }

    date_str = started.strftime("%Y-%m-%d")
    epoch_ms = int(time.time() * 1000)
    key = f"quality/{date_str}_{suite_name}_{epoch_ms}.json"
    try:
        r2_client.upload_json(report, key)
        logger.info(f"Wrote quality report to {key}")
    except Exception as e:
        logger.warning(f"Failed to upload quality report (continuing): {e}")

    # Pretty-print summary to stdout
    print("\n" + "=" * 72)
    print(f"Quality report: {suite_name}  ({summary['passed']}/{summary['total']} passed)")
    print("=" * 72)
    for r in results:
        icon = "✓" if r.status == "pass" else ("✗" if r.severity == "critical" else "!")
        print(f"  [{icon}] {r.severity:<8} {r.name:<55} {r.message}")
    print("=" * 72)
    print(f"critical failed: {summary['critical_failed']}  |  warn failed: {summary['warn_failed']}")
    print("=" * 72 + "\n")

    return 1 if summary["critical_failed"] > 0 else 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite", choices=["daily", "weekly", "full"], required=True)
    args = parser.parse_args()
    sys.exit(run(args.suite))


if __name__ == "__main__":
    main()
