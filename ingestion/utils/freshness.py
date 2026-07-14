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

TTL jitter: the initial backfill pulled the whole universe in the same week,
so with a fixed TTL every symbol expires in the same week too — one weekly
run every ~9 weeks re-fetches everything at once (hours of extra API time)
and re-synchronizes the herd for the next cycle. To break that up, each
symbol's effective TTL is offset by a deterministic per-symbol jitter of up
to ±JITTER_FRACTION, spreading expirations across a multi-week window while
keeping every symbol's refresh cadence stable. Short TTLs (< JITTER_MIN_TTL_DAYS)
are left exact: nudging a 6-day TTL past the 7-day cron interval would make
weekly-fresh endpoints skip alternate runs.
"""
from __future__ import annotations

import logging
import zlib
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

# Maximum per-symbol TTL offset, as a fraction of the endpoint TTL.
# 60d TTL → ±9d (effective 51–69d); 75d → ±11d; 25d → ±3d.
JITTER_FRACTION = 0.15
# TTLs shorter than this get no jitter (see module docstring).
JITTER_MIN_TTL_DAYS = 20


def symbol_jitter_days(symbol: str, ttl_days: int) -> int:
    """Deterministic per-symbol TTL offset in [-max_jitter, +max_jitter].

    Uses crc32 rather than hash(): Python salts str hashes per process, and
    the offset must be identical on every run or a symbol's effective TTL
    would wander week to week.
    """
    if ttl_days < JITTER_MIN_TTL_DAYS:
        return 0
    max_jitter = round(ttl_days * JITTER_FRACTION)
    span = 2 * max_jitter + 1
    return zlib.crc32(symbol.encode("utf-8")) % span - max_jitter


def build_fresh_symbol_set(
    bronze_dir: str,
    ttl_days: int,
    today: date | None = None,
) -> set[str]:
    """Return symbols with a bronze key younger than their effective TTL
    (`ttl_days` plus the per-symbol jitter offset).

    One `r2_client.list_keys` call per invocation. Callers pass the leaf
    directory name (e.g. "income_statement"), not the full prefix.

    Keys with unparseable date components are ignored. If the bronze prefix
    is empty, returns an empty set — every symbol will be re-pulled.
    """
    today = today or date.today()
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

    fresh = {
        sym
        for sym, d in latest_per_symbol.items()
        if d >= today - timedelta(days=ttl_days + symbol_jitter_days(sym, ttl_days))
    }
    logger.info(
        f"freshness gate: {bronze_dir} TTL={ttl_days}d (±{round(ttl_days * JITTER_FRACTION) if ttl_days >= JITTER_MIN_TTL_DAYS else 0}d jitter) — "
        f"{len(latest_per_symbol)} symbols in bronze, {len(fresh)} still fresh"
    )
    return fresh
