"""
Build gold/fact_valuation_daily/year=YYYY/fact_valuation_daily.parquet

One row per (symbol, trade_date). Joins fact_prices_enriched with the
most-recent point-in-time fundamentals (as-of `report_date`) and dim_company
metadata, then derives daily valuation ratios + the Eddy Elfenbein fair-PE
heuristic.

Cadence: daily, in daily_prices.yml AFTER build_prices_enriched.

Strategy:
- Heavy join + arithmetic done in DuckDB SQL (server-side) to scale beyond the
  current 107-ticker universe toward full US-listed (~8K tickers × ~5K days).
  Pandas only handles the per-year partitioning and write.
- ASOF JOIN onto fundamentals keyed by (symbol, report_date <= trade_date).
  Fallback: when report_date is null, approximate with fiscal_date_ending + 60d.
  This prevents look-ahead bias — a daily row dated 2024-03-15 only sees
  fundamentals that were publicly reported on or before that day.
- Defensive QUALIFY ROW_NUMBER()=1 on each gold source to survive duplicates.
- market_cap uses dim_company.shares_outstanding (point-in-time snapshot, same
  compromise as dim_company_enriched.market_cap_latest). True historical
  shares would require joining bronze/shares_outstanding by report_date —
  noted as a Phase C upgrade.
- Elfenbein formula: fair_pe = growth_pct/2 + 8 where growth_pct is the
  trailing 5y (or 3y) EPS CAGR expressed in percent. Null when CAGR is
  negative or missing (formula breaks for declining EPS).

Run as:  python -m transform_gold.build_valuation_daily
"""

from __future__ import annotations

import io
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from ingestion.utils import r2_client  # noqa: E402
from transform_gold.utils.duckdb_silver import (  # noqa: E402
    duckdb_to_r2, silver_scan, gold_scan,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

OUT_PREFIX = "gold/fact_valuation_daily"


SQL = """
WITH joined AS (
    SELECT
        p.symbol,
        p.trade_date,
        p.close,
        p.adjusted_close,
        d.sector,
        d.industry,
        d.shares_outstanding,
        f.fiscal_date_ending           AS fundamentals_as_of,
        f.effective_date               AS fundamentals_effective_date,
        f.reported_eps_ttm,
        f.total_revenue_ttm,
        f.ebitda_ttm,
        f.free_cash_flow_ttm,
        f.dividend_payout_ttm,
        f.total_equity,
        f.long_term_debt,
        f.short_term_debt,
        f.cash_and_equivalents,
        f.short_term_investments,
        f.eps_cagr_5y,
        f.eps_cagr_3y
    FROM prices p
    LEFT JOIN dim d USING (symbol)
    ASOF LEFT JOIN funda_q f
      ON p.symbol = f.symbol
     AND p.trade_date >= f.effective_date
    WHERE EXTRACT(YEAR FROM p.trade_date) = {year}
),
derived AS (
    SELECT
        symbol,
        trade_date,
        close,
        adjusted_close,
        sector,
        industry,
        shares_outstanding,
        fundamentals_as_of,
        fundamentals_effective_date,
        reported_eps_ttm,

        -- Market cap and enterprise value
        close * shares_outstanding AS market_cap,
        close * shares_outstanding
            + COALESCE(long_term_debt, 0) + COALESCE(short_term_debt, 0)
            - COALESCE(cash_and_equivalents, 0) - COALESCE(short_term_investments, 0)
        AS enterprise_value,

        -- Valuation ratios — null when denominator <= 0 or null
        CASE WHEN reported_eps_ttm > 0 THEN close / reported_eps_ttm END AS pe_ttm,
        CASE WHEN total_revenue_ttm > 0
             THEN (close * shares_outstanding) / total_revenue_ttm END AS ps_ttm,
        CASE WHEN total_equity > 0
             THEN (close * shares_outstanding) / total_equity END AS pb,
        CASE WHEN ebitda_ttm > 0
             THEN (close * shares_outstanding
                   + COALESCE(long_term_debt, 0) + COALESCE(short_term_debt, 0)
                   - COALESCE(cash_and_equivalents, 0)
                   - COALESCE(short_term_investments, 0)) / ebitda_ttm
             END AS ev_ebitda_ttm,
        CASE WHEN (close * shares_outstanding) > 0
             THEN free_cash_flow_ttm / (close * shares_outstanding) END AS fcf_yield_ttm,
        CASE WHEN (close * shares_outstanding) > 0
             THEN dividend_payout_ttm / (close * shares_outstanding) END AS dividend_yield_ttm,

        eps_cagr_5y,
        eps_cagr_3y,

        -- Elfenbein fair P/E: growth_pct / 2 + 8.  Null when growth is null
        -- or negative (formula breaks for declining EPS).
        CASE WHEN eps_cagr_5y IS NOT NULL AND eps_cagr_5y >= 0
             THEN (eps_cagr_5y * 100.0) / 2.0 + 8.0 END AS elfenbein_fair_pe,
        CASE WHEN eps_cagr_3y IS NOT NULL AND eps_cagr_3y >= 0
             THEN (eps_cagr_3y * 100.0) / 2.0 + 8.0 END AS elfenbein_fair_pe_3y
    FROM joined
)
SELECT
    symbol,
    trade_date,
    close,
    adjusted_close,
    sector,
    industry,
    shares_outstanding,
    fundamentals_as_of,
    fundamentals_effective_date,
    reported_eps_ttm,

    market_cap,
    enterprise_value,
    pe_ttm,
    ps_ttm,
    pb,
    ev_ebitda_ttm,
    fcf_yield_ttm,
    dividend_yield_ttm,

    eps_cagr_5y,
    eps_cagr_3y,
    elfenbein_fair_pe,
    elfenbein_fair_pe_3y,

    -- fair_price = fair_pe × EPS_ttm.  Null when EPS_ttm <= 0.
    CASE WHEN elfenbein_fair_pe IS NOT NULL AND reported_eps_ttm > 0
         THEN elfenbein_fair_pe * reported_eps_ttm END AS elfenbein_fair_price,
    CASE WHEN elfenbein_fair_pe_3y IS NOT NULL AND reported_eps_ttm > 0
         THEN elfenbein_fair_pe_3y * reported_eps_ttm END AS elfenbein_fair_price_3y,

    -- upside_pct and margin_of_safety_met, derived from the 5y variant
    CASE WHEN elfenbein_fair_pe IS NOT NULL AND reported_eps_ttm > 0 AND close > 0
         THEN (elfenbein_fair_pe * reported_eps_ttm - close) / close END AS elfenbein_upside_pct,
    CASE WHEN elfenbein_fair_pe IS NOT NULL AND reported_eps_ttm > 0
         THEN close <= 0.7 * elfenbein_fair_pe * reported_eps_ttm END AS elfenbein_margin_of_safety_met
FROM derived
"""


def write_by_year(df: pd.DataFrame) -> None:
    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["_year"] = df["trade_date"].dt.year
    df["trade_date"] = df["trade_date"].dt.date

    for year, year_df in df.groupby("_year"):
        year_df = year_df.drop(columns=["_year"])
        for col in year_df.columns:
            if "date" in col.lower() and col != "trade_date":
                year_df[col] = pd.to_datetime(year_df[col], errors="coerce").dt.date

        table = pa.Table.from_pandas(year_df, preserve_index=False)
        new_fields = []
        for f in table.schema:
            if "date" in f.name.lower() and f.type != pa.date32():
                new_fields.append(pa.field(f.name, pa.date32()))
            else:
                new_fields.append(f)
        table = table.cast(pa.schema(new_fields))

        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        key = f"{OUT_PREFIX}/year={int(year)}/fact_valuation_daily.parquet"
        r2_client.upload_bytes(buf.getvalue(), key)
        logger.info(f"Wrote {len(year_df):,} rows → {key}")


def main():
    bucket = os.environ["R2_BUCKET_NAME"]
    con = duckdb_to_r2()

    # The ASOF JOIN below explodes in memory at full-universe scale (~3M price
    # rows × per-row lookup into ~100K wide fundamentals rows). Cap the
    # in-memory working set and let DuckDB spill to local disk.
    tmp_dir = Path("tmp_duckdb").resolve()
    tmp_dir.mkdir(exist_ok=True)
    con.execute("SET memory_limit = '6GB'")
    con.execute(f"SET temp_directory = '{tmp_dir.as_posix()}'")
    con.execute("SET preserve_insertion_order = false")

    # Materialize the three inputs as local DuckDB tables before the heavy ASOF
    # JOIN. Reading them once up-front avoids DuckDB issuing many small ranged
    # GETs against R2 during the join, which was making the join effectively
    # never finish when run live against httpfs.
    logger.info("Materializing prices into local DuckDB table...")
    con.execute(f"""
        CREATE TEMP TABLE prices_raw AS
        SELECT symbol, trade_date, close, adjusted_close, gold_built_utc
        FROM {gold_scan(bucket, "fact_prices_enriched/**/*.parquet")}
    """)
    con.execute("""
        CREATE TEMP TABLE prices AS
        SELECT symbol, trade_date, close, adjusted_close
        FROM prices_raw
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol, trade_date ORDER BY gold_built_utc DESC NULLS LAST
        ) = 1
    """)
    logger.info(
        f"  prices: {con.execute('SELECT COUNT(*) FROM prices').fetchone()[0]:,} rows"
    )

    logger.info("Materializing fundamentals_wide into local DuckDB table...")
    con.execute(f"""
        CREATE TEMP TABLE funda_q AS
        SELECT
            symbol,
            fiscal_date_ending,
            COALESCE(report_date, fiscal_date_ending + INTERVAL '60 days') AS effective_date,
            reported_eps_ttm,
            total_revenue_ttm,
            ebitda_ttm,
            free_cash_flow_ttm,
            dividend_payout_ttm,
            total_equity,
            long_term_debt,
            short_term_debt,
            cash_and_equivalents,
            short_term_investments,
            eps_cagr_5y,
            eps_cagr_3y
        FROM {gold_scan(bucket, "fact_fundamentals_wide/*.parquet")}
        WHERE period_type = 'quarterly'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol, fiscal_date_ending
            ORDER BY gold_built_utc DESC NULLS LAST
        ) = 1
    """)
    logger.info(
        f"  funda_q: {con.execute('SELECT COUNT(*) FROM funda_q').fetchone()[0]:,} rows"
    )

    logger.info("Materializing dim_company into local DuckDB table...")
    con.execute(f"""
        CREATE TEMP TABLE dim AS
        SELECT symbol, sector, industry, shares_outstanding
        FROM {silver_scan(bucket, "dim_company/*.parquet")}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol ORDER BY last_updated DESC NULLS LAST
        ) = 1
    """)
    logger.info(
        f"  dim: {con.execute('SELECT COUNT(*) FROM dim').fetchone()[0]:,} rows"
    )

    # Run the ASOF join one year at a time. A single full-universe pass OOMs;
    # per-year passes have a small enough working set to complete in seconds.
    years = [r[0] for r in con.execute(
        "SELECT DISTINCT EXTRACT(YEAR FROM trade_date)::INT AS y "
        "FROM prices ORDER BY y"
    ).fetchall()]
    logger.info(f"Will process {len(years)} years: {years[0]}..{years[-1]}")

    built_utc = datetime.now(timezone.utc).isoformat()
    total_rows = 0
    total_pe = 0
    total_elf = 0
    for year in years:
        logger.info(f"[year={year}] Running join + valuation SQL...")
        df = con.execute(SQL.format(year=year)).df()
        if df.empty:
            logger.info(f"[year={year}] no rows, skipping")
            continue
        df["gold_built_utc"] = built_utc
        pe_coverage = df["pe_ttm"].notna().sum()
        elf_coverage = df["elfenbein_fair_price"].notna().sum()
        total_rows += len(df)
        total_pe += pe_coverage
        total_elf += elf_coverage
        logger.info(
            f"[year={year}] {len(df):,} rows, {df['symbol'].nunique()} symbols, "
            f"pe_ttm={pe_coverage:,} elfenbein={elf_coverage:,}"
        )
        write_by_year(df)
        del df

    logger.info(
        f"Total: {total_rows:,} rows; PE_TTM populated on {total_pe:,}; "
        f"Elfenbein fair_price on {total_elf:,}"
    )


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("build_valuation_daily"):
        main()
