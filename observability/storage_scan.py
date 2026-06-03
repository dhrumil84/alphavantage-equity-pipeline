"""
Walk R2 bucket and record storage usage by layer/dataset.

Writes a daily snapshot to:

    metrics/storage/{YYYY-MM-DD}.json

Idempotent per day (re-running overwrites the same key — that's fine,
the second snapshot is just newer).

Run as:  python -m observability.storage_scan

The output structure:

    {
      "snapshot_utc": "...",
      "totals": {"objects": N, "bytes": N, "gb": N},
      "by_layer": {
        "bronze": {"objects": ..., "bytes": ..., "gb": ...},
        "silver": {...},
        ...
      },
      "by_dataset": {
        "bronze/daily_prices": {"objects": ..., "bytes": ...},
        "bronze/income_statement": {...},
        "silver/fact_daily_prices": {...},
        ...
      }
    }
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone

from ingestion.utils import r2_client

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Top-level prefixes we recognise. Anything else is bucketed under "other".
KNOWN_LAYERS = ["bronze", "silver", "gold", "config", "reports", "metrics", "quality"]


def scan() -> dict:
    client = r2_client._get_client()
    bucket = r2_client._get_bucket()

    by_layer = defaultdict(lambda: {"objects": 0, "bytes": 0})
    by_dataset = defaultdict(lambda: {"objects": 0, "bytes": 0})
    total_objects = 0
    total_bytes = 0

    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket)

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj["Size"]
            total_objects += 1
            total_bytes += size

            parts = key.split("/", 2)
            layer = parts[0] if parts[0] in KNOWN_LAYERS else "other"
            dataset = f"{parts[0]}/{parts[1]}" if len(parts) >= 2 else parts[0]

            by_layer[layer]["objects"] += 1
            by_layer[layer]["bytes"] += size
            by_dataset[dataset]["objects"] += 1
            by_dataset[dataset]["bytes"] += size

    def with_gb(d):
        return {**d, "gb": round(d["bytes"] / (1024**3), 4)}

    return {
        "snapshot_utc": datetime.now(timezone.utc).isoformat(),
        "totals": with_gb({"objects": total_objects, "bytes": total_bytes}),
        "by_layer": {k: with_gb(v) for k, v in sorted(by_layer.items())},
        "by_dataset": {k: with_gb(v) for k, v in sorted(by_dataset.items())},
    }


def main():
    logger.info("Scanning R2 bucket for storage metrics...")
    snapshot = scan()

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"metrics/storage/{date_str}.json"
    r2_client.upload_json(snapshot, key)

    totals = snapshot["totals"]
    logger.info(
        f"Wrote storage snapshot to {key} — "
        f"{totals['objects']:,} objects, {totals['gb']} GB"
    )
    for layer, stats in snapshot["by_layer"].items():
        logger.info(f"  {layer}: {stats['objects']:,} objects, {stats['gb']} GB")


if __name__ == "__main__":
    main()
