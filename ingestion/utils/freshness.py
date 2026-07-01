"""
Freshness gating for per-symbol bronze ingestion.

Each ingest script writes today's data to bronze/{dir}/{symbol}/{date}.json.
The existing per-key `r2_client.key_exists()` idempotency check prevents
re-fetching within the same day, but on the next weekly run every key is
"not yet pulled today" and everything re-fetches from scratch. That's fine
for daily-fresh data (news, insider filings, corporate actions) but wasteful
for quarterly-updated data (fundamentals, 13F, overview) — those change on
filing schedules, not on our cron.

Per-endpoint TTLs below reflect the underlying update cadence. On each run,
we list keys under `bronze/{dir}/` once, determine the most recent pull date
per symbol, and skip symbols whose newest key is younger than the TTL.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

from ingestion.utils import r2_client

logger = logging.getLogger(__name__)

# TTL in days per bronze directory. Keys are the {dir_name} used in
# bronze/{dir_name}/{symbol}/{YYYY-MM-DD}.json paths.
ENDPOINT_TTL_DAYS: dict[str, int] = {
    # Quarterly-ish
    "company_overview": 75,
    "income_statement": 60,
    "balance_sheet": 60,
    "cash_flow": 60,
    "earnings": 60,
    "institutional_holdings": 60,
    # Monthly-ish
    "etf_profile": 25,
    "shares_outstanding": 25,
    # Weekly-fresh
    "dividends": 6,
    "splits": 6,
    "news_sentiment_ticker": 6,
    "insider_transactions": 6,
}


def build_fresh_symbol_set(
    bronze_dir: str,
    ttl_days: int,
    today: date | None = None,
) -> set[str]:
    """Return symbols with a bronze key younger than `ttl_days`.

    One `r2_client.list_keys` call per invocation. Callers pass the leaf
    directory name (e.g. "income_statement"), not the full prefix.

    Keys with unparseable date components are ignored. If the bronze prefix
    is empty, returns an empty set — every symbol will be re-pulled.
    """
    today = today or date.today()
    cutoff = today - timedelta(days=ttl_days)
    prefix = f"bronze/{bronze_dir}/"
    all_keys = r2_client.list_keys(prefix)

    latest_per_symbol: dict[str, date] = {}
    for k in all_keys:
        parts = k.split("/")
        # Expected: bronze/{bronze_dir}/{symbol}/{YYYY-MM-DD}.json
        if len(parts) < 4:
            continue
        symbol = parts[2]
        stem = parts[3].rsplit(".", 1)[0]
        try:
            key_date = date.fromisoformat(stem)
        except ValueError:
            continue
        prev = latest_per_symbol.get(symbol)
        if prev is None or key_date > prev:
            latest_per_symbol[symbol] = key_date

    fresh = {sym for sym, d in latest_per_symbol.items() if d >= cutoff}
    logger.info(
        f"freshness gate: {bronze_dir} TTL={ttl_days}d — "
        f"{len(latest_per_symbol)} symbols in bronze, {len(fresh)} still fresh"
    )
    return fresh
