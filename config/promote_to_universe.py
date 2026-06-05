"""
Promote symbols from the ITOT/SPTM anchor (config/vti_universe.csv) into the
working ticker_universe.csv that the ingestion + transform pipeline reads.

Designed for staged expansion. You ramp via the --top-n flag:

    # Add top-500 by weight to ticker_universe.csv (in addition to whatever
    # is already there). Idempotent — re-running won't duplicate.
    python -m config.promote_to_universe --top-n 500

    # Eventually: promote everything in SPTM.
    python -m config.promote_to_universe --top-n 9999

The first run on a new tier should be followed by a `full`-mode daily-prices
backfill so the newly-added symbols get their 20-year history seeded. After
that, incremental mode picks them up automatically.

This script never removes symbols. If you want to prune (e.g. drop a stale
hand-added ticker that's no longer in SPTM), edit ticker_universe.csv by hand.
"""
import argparse
import csv
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--top-n", type=int, default=500,
                        help="Promote the top-N SPTM holdings by weight (default 500)")
    args = parser.parse_args()

    vti_path = Path("config") / "vti_universe.csv"
    universe_path = Path("config") / "ticker_universe.csv"

    if not vti_path.exists():
        raise FileNotFoundError(f"{vti_path} not found — run ingest_itot_holdings first")

    vti = pd.read_csv(vti_path)
    vti = vti.dropna(subset=["weight"]).sort_values("weight", ascending=False)
    picked = vti.head(args.top_n)
    logger.info(f"Selected top {len(picked)} symbols by weight from {vti_path}")
    logger.info(
        f"  Weight coverage: {picked['weight'].sum():.1f}% of SPTM "
        f"(vs. {vti['weight'].sum():.1f}% for full anchor)"
    )

    existing_symbols: set[str] = set()
    existing_rows: list[dict] = []
    if universe_path.exists():
        with open(universe_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_rows.append(row)
                existing_symbols.add(row["symbol"].strip().upper())
        logger.info(f"Loaded {len(existing_symbols)} existing symbols from {universe_path}")

    new_rows = []
    for _, r in picked.iterrows():
        sym = str(r["symbol"]).strip().upper()
        if sym in existing_symbols:
            continue
        new_rows.append({"symbol": sym, "name": str(r["name"]), "active": "true"})

    if not new_rows:
        logger.info("No new symbols to add — universe already covers the requested tier.")
        return

    out_rows = existing_rows + new_rows
    with open(universe_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["symbol", "name", "active"])
        writer.writeheader()
        writer.writerows(out_rows)

    logger.info(f"Appended {len(new_rows)} new symbols. Universe now {len(out_rows)} rows.")
    logger.info("Sample of newly added:")
    for r in new_rows[:10]:
        logger.info(f"  + {r['symbol']:<8} {r['name']}")
    logger.info("")
    logger.info("Next: run `python -m ingestion.ingest_daily_prices --mode full` to seed")
    logger.info("history for the new symbols (incremental mode skips backfill).")


if __name__ == "__main__":
    main()
