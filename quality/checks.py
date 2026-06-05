"""
Data-quality checks for the silver layer.

Each check is a function that takes a DuckDB connection (wired to R2) and
returns a CheckResult. Severity controls runner exit behaviour:

  - critical: failure causes runner to exit non-zero (workflow goes red)
  - warn:     recorded in the report but does not fail the workflow
  - info:     metric-only, never fails

Default thresholds (rationale in PROJECT_BRIEF / discussion):
  - adjusted_close null rate > 1%       → critical
  - daily prices freshness > 5 trading days → critical
  - daily prices freshness > 2 trading days → warn
  - fundamentals symbol coverage < 80% of active universe → warn
"""

from __future__ import annotations

from dataclasses import dataclass, asdict, field
from datetime import date, timedelta
from typing import Any, Callable

import duckdb


SILVER_ROOT = "s3://{bucket}/silver"
GOLD_ROOT = "s3://{bucket}/gold"


@dataclass
class CheckResult:
    name: str
    severity: str  # 'critical' | 'warn' | 'info'
    status: str    # 'pass' | 'fail'
    details: dict = field(default_factory=dict)
    message: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


# ─── Helpers ────────────────────────────────────────────────────────────────

def _scan(con: duckdb.DuckDBPyConnection, bucket: str, path: str) -> str:
    """Return a read_parquet() SQL expression for a silver table glob."""
    return f"read_parquet('s3://{bucket}/silver/{path}', union_by_name=true)"


def _gold_scan(con: duckdb.DuckDBPyConnection, bucket: str, path: str) -> str:
    return f"read_parquet('s3://{bucket}/gold/{path}', union_by_name=true)"


def _gold_table_exists(con, bucket: str, path: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {_gold_scan(con, bucket, path)} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def _table_exists(con: duckdb.DuckDBPyConnection, bucket: str, path: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {_scan(con, bucket, path)} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


# ─── Individual checks ──────────────────────────────────────────────────────

def check_dim_company_has_rows(con, bucket: str) -> CheckResult:
    n = con.execute(
        f"SELECT COUNT(*) FROM {_scan(con, bucket, 'dim_company/*.parquet')}"
    ).fetchone()[0]
    return CheckResult(
        name="dim_company.row_count",
        severity="critical",
        status="pass" if n > 0 else "fail",
        details={"row_count": n},
        message=f"dim_company has {n:,} rows",
    )


def check_dim_company_unique_symbol(con, bucket: str) -> CheckResult:
    dupes = con.execute(f"""
        SELECT symbol, COUNT(*) c
        FROM {_scan(con, bucket, 'dim_company/*.parquet')}
        GROUP BY symbol HAVING c > 1
    """).fetchall()
    return CheckResult(
        name="dim_company.symbol_unique",
        severity="critical",
        status="pass" if len(dupes) == 0 else "fail",
        details={"duplicate_count": len(dupes), "examples": [d[0] for d in dupes[:5]]},
        message=(
            "no duplicate symbols in dim_company"
            if len(dupes) == 0
            else f"{len(dupes)} duplicate symbols in dim_company"
        ),
    )


def check_daily_prices_has_rows(con, bucket: str) -> CheckResult:
    n = con.execute(
        f"SELECT COUNT(*) FROM {_scan(con, bucket, 'fact_daily_prices/**/*.parquet')}"
    ).fetchone()[0]
    return CheckResult(
        name="fact_daily_prices.row_count",
        severity="critical",
        status="pass" if n > 0 else "fail",
        details={"row_count": n},
        message=f"fact_daily_prices has {n:,} rows",
    )


def check_adjusted_close_null_rate(con, bucket: str, max_pct: float = 1.0) -> CheckResult:
    row = con.execute(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN adjusted_close IS NULL THEN 1 ELSE 0 END) AS nulls
        FROM {_scan(con, bucket, 'fact_daily_prices/**/*.parquet')}
    """).fetchone()
    total, nulls = row[0] or 0, row[1] or 0
    pct = (nulls / total * 100) if total else 0.0
    return CheckResult(
        name="fact_daily_prices.adjusted_close_null_rate",
        severity="critical",
        status="pass" if pct <= max_pct else "fail",
        details={"total_rows": total, "null_rows": nulls, "null_pct": round(pct, 4),
                 "threshold_pct": max_pct},
        message=f"adjusted_close null rate = {pct:.3f}% (threshold {max_pct}%)",
    )


def check_daily_prices_freshness(con, bucket: str, today: date | None = None,
                                  warn_days: int = 2, fail_days: int = 5) -> list[CheckResult]:
    """Two checks: freshness warn (2d) and freshness critical (5d) — trading days,
    approximated as calendar days excluding Sat/Sun."""
    today = today or date.today()
    max_trade_date = con.execute(
        f"SELECT MAX(trade_date) FROM {_scan(con, bucket, 'fact_daily_prices/**/*.parquet')}"
    ).fetchone()[0]

    if max_trade_date is None:
        result_common = {
            "max_trade_date": None,
            "today": today.isoformat(),
            "trading_days_stale": None,
        }
        return [
            CheckResult("fact_daily_prices.freshness_warn", "warn", "fail",
                        result_common, "no trade dates found"),
            CheckResult("fact_daily_prices.freshness_critical", "critical", "fail",
                        result_common, "no trade dates found"),
        ]

    # Count trading days (Mon-Fri) between max_trade_date and today, exclusive of max_trade_date
    stale = 0
    cursor = max_trade_date
    while cursor < today:
        cursor = cursor + timedelta(days=1)
        if cursor.weekday() < 5:  # 0=Mon..4=Fri
            stale += 1

    common = {
        "max_trade_date": max_trade_date.isoformat(),
        "today": today.isoformat(),
        "trading_days_stale": stale,
    }
    return [
        CheckResult(
            name="fact_daily_prices.freshness_warn",
            severity="warn",
            status="pass" if stale <= warn_days else "fail",
            details={**common, "threshold_days": warn_days},
            message=f"prices stale by {stale} trading day(s); warn threshold {warn_days}",
        ),
        CheckResult(
            name="fact_daily_prices.freshness_critical",
            severity="critical",
            status="pass" if stale <= fail_days else "fail",
            details={**common, "threshold_days": fail_days},
            message=f"prices stale by {stale} trading day(s); fail threshold {fail_days}",
        ),
    ]


def check_table_has_rows(con, bucket: str, table_path: str, name: str,
                          severity: str = "warn") -> CheckResult:
    if not _table_exists(con, bucket, table_path):
        return CheckResult(name=f"{name}.row_count", severity=severity, status="fail",
                            details={"row_count": 0}, message=f"{name} not found in silver")
    n = con.execute(f"SELECT COUNT(*) FROM {_scan(con, bucket, table_path)}").fetchone()[0]
    return CheckResult(
        name=f"{name}.row_count",
        severity=severity,
        status="pass" if n > 0 else "fail",
        details={"row_count": n},
        message=f"{name} has {n:,} rows",
    )


def check_fundamentals_coverage(con, bucket: str, table_path: str, name: str,
                                  min_pct: float = 80.0) -> CheckResult:
    """% of active dim_company symbols that appear at least once in the given fundamentals table."""
    if not _table_exists(con, bucket, table_path):
        return CheckResult(name=f"{name}.coverage", severity="warn", status="fail",
                            details={}, message=f"{name} not found in silver")
    row = con.execute(f"""
        WITH active AS (
            SELECT DISTINCT symbol FROM {_scan(con, bucket, 'dim_company/*.parquet')}
            WHERE listing_status = 'active'
        ),
        covered AS (
            SELECT DISTINCT symbol FROM {_scan(con, bucket, table_path)}
        )
        SELECT
            (SELECT COUNT(*) FROM active) AS active_n,
            (SELECT COUNT(*) FROM active a INNER JOIN covered c USING (symbol)) AS covered_n
    """).fetchone()
    active_n, covered_n = row[0] or 0, row[1] or 0
    pct = (covered_n / active_n * 100) if active_n else 0.0
    return CheckResult(
        name=f"{name}.coverage",
        severity="warn",
        status="pass" if pct >= min_pct else "fail",
        details={"active_symbols": active_n, "covered_symbols": covered_n,
                 "coverage_pct": round(pct, 2), "threshold_pct": min_pct},
        message=f"{name} covers {pct:.1f}% of active universe (threshold {min_pct}%)",
    )


def check_schema(con, bucket: str, table_path: str, name: str,
                 required_cols: list[str]) -> CheckResult:
    """Verify required columns exist in the silver table (schema drift detection)."""
    if not _table_exists(con, bucket, table_path):
        return CheckResult(name=f"{name}.schema", severity="critical", status="fail",
                            details={"required": required_cols},
                            message=f"{name} not found in silver")
    cols = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM {_scan(con, bucket, table_path)} LIMIT 0"
    ).fetchall()}
    missing = [c for c in required_cols if c not in cols]
    return CheckResult(
        name=f"{name}.schema",
        severity="critical",
        status="pass" if not missing else "fail",
        details={"required": required_cols, "missing": missing,
                 "actual_columns": sorted(cols)},
        message=("all required columns present" if not missing
                  else f"missing columns: {missing}"),
    )


# ─── Suite definitions ──────────────────────────────────────────────────────

REQUIRED_COLS = {
    "fact_daily_prices": ["symbol", "trade_date", "adjusted_close", "close", "volume", "pull_date"],
    "fact_income_statement": ["symbol", "fiscal_date_ending", "period_type", "total_revenue", "net_income"],
    "fact_balance_sheet": ["symbol", "fiscal_date_ending", "period_type", "total_assets", "total_equity"],
    "fact_cash_flow": ["symbol", "fiscal_date_ending", "period_type", "operating_cashflow", "free_cash_flow"],
    "fact_earnings": ["symbol", "fiscal_date_ending", "period_type", "reported_eps", "report_date"],
    "dim_company": ["symbol", "name", "sector", "listing_status", "ipo_date"],
}


def check_gold_table_has_rows(con, bucket: str, path: str, name: str,
                                severity: str = "critical") -> CheckResult:
    if not _gold_table_exists(con, bucket, path):
        return CheckResult(name=f"gold.{name}.row_count", severity=severity, status="fail",
                            details={"row_count": 0}, message=f"gold/{name} not found")
    n = con.execute(f"SELECT COUNT(*) FROM {_gold_scan(con, bucket, path)}").fetchone()[0]
    return CheckResult(
        name=f"gold.{name}.row_count",
        severity=severity,
        status="pass" if n > 0 else "fail",
        details={"row_count": n},
        message=f"gold/{name} has {n:,} rows",
    )


def check_gold_valuation_daily_coverage(con, bucket: str) -> CheckResult:
    """Every ticker present in fact_prices_enriched should be present in
    fact_valuation_daily. Catches catastrophic join failure regardless of
    universe size (scales from 107 → 8K tickers without retuning)."""
    if not _gold_table_exists(con, bucket, "fact_valuation_daily/**/*.parquet"):
        return CheckResult(
            name="gold.fact_valuation_daily.symbol_coverage",
            severity="critical", status="fail",
            details={}, message="gold/fact_valuation_daily not found",
        )
    row = con.execute(f"""
        WITH p AS (
            SELECT DISTINCT symbol FROM {_gold_scan(con, bucket, 'fact_prices_enriched/**/*.parquet')}
        ),
        v AS (
            SELECT DISTINCT symbol FROM {_gold_scan(con, bucket, 'fact_valuation_daily/**/*.parquet')}
        )
        SELECT
            (SELECT COUNT(*) FROM p) AS prices_n,
            (SELECT COUNT(*) FROM p INNER JOIN v USING (symbol)) AS covered_n
    """).fetchone()
    prices_n, covered_n = row[0] or 0, row[1] or 0
    missing = prices_n - covered_n
    return CheckResult(
        name="gold.fact_valuation_daily.symbol_coverage",
        severity="critical",
        status="pass" if missing == 0 and prices_n > 0 else "fail",
        details={"prices_symbols": prices_n, "valuation_symbols": covered_n,
                 "missing": missing},
        message=f"valuation covers {covered_n}/{prices_n} symbols in prices_enriched",
    )


def check_gold_sector_aggregates_coverage(con, bucket: str) -> CheckResult:
    """Every sector that has ≥1 symbol with a populated TTM revenue value
    (i.e. anything other than an all-ETF sector) should appear in the
    latest aggregate snapshot. Sectors like 'OTHER' that hold only SPY/QQQ
    are correctly excluded — they have no computable metrics."""
    if not _gold_table_exists(con, bucket, "fact_sector_aggregates/*.parquet"):
        return CheckResult(
            name="gold.fact_sector_aggregates.sector_coverage",
            severity="critical", status="fail",
            details={}, message="gold/fact_sector_aggregates not found",
        )
    row = con.execute(f"""
        WITH s AS (
            -- Sectors that contain at least one symbol with a populated
            -- quarterly TTM revenue — anything else has no metrics to aggregate.
            SELECT DISTINCT d.sector
            FROM {_scan(con, bucket, 'dim_company/*.parquet')} d
            INNER JOIN {_gold_scan(con, bucket, 'fact_fundamentals_wide/*.parquet')} f
              ON d.symbol = f.symbol
            WHERE d.sector IS NOT NULL
              AND f.period_type = 'quarterly'
              AND f.total_revenue_ttm IS NOT NULL
        ),
        a AS (
            SELECT DISTINCT group_value AS sector
            FROM {_gold_scan(con, bucket, 'fact_sector_aggregates/*.parquet')}
            WHERE grouping_level = 'sector'
              AND as_of_date = (SELECT MAX(as_of_date)
                                FROM {_gold_scan(con, bucket, 'fact_sector_aggregates/*.parquet')})
        )
        SELECT
            (SELECT COUNT(*) FROM s) AS sector_n,
            (SELECT COUNT(*) FROM s INNER JOIN a USING (sector)) AS covered_n
    """).fetchone()
    sector_n, covered_n = row[0] or 0, row[1] or 0
    return CheckResult(
        name="gold.fact_sector_aggregates.sector_coverage",
        severity="critical",
        status="pass" if covered_n == sector_n and sector_n > 0 else "fail",
        details={"expected_sectors": sector_n, "covered_sectors": covered_n},
        message=f"sector_aggregates covers {covered_n}/{sector_n} sectors with computable metrics",
    )


def check_gold_peer_relative_percentile_bounds(con, bucket: str) -> CheckResult:
    """sector_percentile / industry_percentile must lie in [0, 1]."""
    if not _gold_table_exists(con, bucket, "fact_peer_relative/*.parquet"):
        return CheckResult(
            name="gold.fact_peer_relative.percentile_bounds",
            severity="critical", status="fail",
            details={}, message="gold/fact_peer_relative not found",
        )
    row = con.execute(f"""
        SELECT COUNT(*)
        FROM {_gold_scan(con, bucket, 'fact_peer_relative/*.parquet')}
        WHERE (sector_percentile IS NOT NULL AND (sector_percentile < 0 OR sector_percentile > 1))
           OR (industry_percentile IS NOT NULL AND (industry_percentile < 0 OR industry_percentile > 1))
    """).fetchone()
    bad = row[0] or 0
    return CheckResult(
        name="gold.fact_peer_relative.percentile_bounds",
        severity="critical",
        status="pass" if bad == 0 else "fail",
        details={"out_of_bounds_rows": bad},
        message=f"{bad} percentile rows outside [0, 1]",
    )


def daily_suite(con, bucket: str) -> list[CheckResult]:
    """Checks run after the daily prices pipeline."""
    results = [
        check_daily_prices_has_rows(con, bucket),
        check_schema(con, bucket, "fact_daily_prices/**/*.parquet",
                     "fact_daily_prices", REQUIRED_COLS["fact_daily_prices"]),
        check_adjusted_close_null_rate(con, bucket),
        *check_daily_prices_freshness(con, bucket),
        check_gold_table_has_rows(con, bucket,
                                   "fact_prices_enriched/**/*.parquet",
                                   "fact_prices_enriched"),
        check_gold_table_has_rows(con, bucket,
                                   "fact_valuation_daily/**/*.parquet",
                                   "fact_valuation_daily"),
        check_gold_valuation_daily_coverage(con, bucket),
    ]
    return results


def weekly_suite(con, bucket: str) -> list[CheckResult]:
    """Checks run after the weekly fundamentals refresh."""
    results = [
        check_dim_company_has_rows(con, bucket),
        check_dim_company_unique_symbol(con, bucket),
        check_schema(con, bucket, "dim_company/*.parquet",
                     "dim_company", REQUIRED_COLS["dim_company"]),
    ]
    for table, name in [
        ("fact_income_statement/*.parquet", "fact_income_statement"),
        ("fact_balance_sheet/*.parquet", "fact_balance_sheet"),
        ("fact_cash_flow/*.parquet", "fact_cash_flow"),
        ("fact_earnings/*.parquet", "fact_earnings"),
    ]:
        results.append(check_table_has_rows(con, bucket, table, name, severity="warn"))
        results.append(check_schema(con, bucket, table, name, REQUIRED_COLS[name]))
        results.append(check_fundamentals_coverage(con, bucket, table, name))

    # Gold weekly tables
    results.append(check_gold_table_has_rows(
        con, bucket, "fact_fundamentals_wide/*.parquet", "fact_fundamentals_wide"))
    results.append(check_gold_table_has_rows(
        con, bucket, "dim_company_enriched/*.parquet", "dim_company_enriched"))
    results.append(check_gold_table_has_rows(
        con, bucket, "fact_sector_aggregates/*.parquet", "fact_sector_aggregates"))
    results.append(check_gold_table_has_rows(
        con, bucket, "fact_peer_relative/*.parquet", "fact_peer_relative"))
    results.append(check_gold_sector_aggregates_coverage(con, bucket))
    results.append(check_gold_peer_relative_percentile_bounds(con, bucket))
    return results


def full_suite(con, bucket: str) -> list[CheckResult]:
    return daily_suite(con, bucket) + weekly_suite(con, bucket)
