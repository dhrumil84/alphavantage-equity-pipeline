"""
Ensure benchmark tickers (SPY, QQQ, optionally others) are in
config/ticker_universe.csv. Idempotent: re-running is a no-op if all
benchmarks are already present.

Run from project root:
    python -m config.ensure_benchmarks
"""

from __future__ import annotations

import csv
from pathlib import Path

BENCHMARKS: list[tuple[str, str]] = [
    ("SPY", "SPDR S&P 500 ETF Trust"),
    ("QQQ", "Invesco QQQ Trust"),
]

UNIVERSE_PATH = Path(__file__).resolve().parent / "ticker_universe.csv"


def main() -> None:
    with UNIVERSE_PATH.open("r", newline="") as f:
        rows = list(csv.DictReader(f))
        fieldnames = rows[0].keys() if rows else ["symbol", "name", "active"]

    existing = {r["symbol"].upper() for r in rows}
    added: list[str] = []

    for symbol, name in BENCHMARKS:
        if symbol in existing:
            print(f"  [skip] {symbol} already present")
            continue
        rows.append({"symbol": symbol, "name": name, "active": "true"})
        added.append(symbol)
        print(f"  [add]  {symbol} — {name}")

    if not added:
        print("All benchmarks already in universe — no changes.")
        return

    with UNIVERSE_PATH.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nAdded {len(added)} benchmark ticker(s): {', '.join(added)}")
    print(f"Universe now has {len(rows)} tickers.")


if __name__ == "__main__":
    main()
