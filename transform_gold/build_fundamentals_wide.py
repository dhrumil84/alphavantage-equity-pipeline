"""
Build gold/fact_fundamentals_wide/fact_fundamentals_wide.parquet

One row per (symbol, fiscal_date_ending, period_type).
Combines income statement + balance sheet + cash flow + earnings,
plus derived ratios, YoY/QoQ growth, TTM aggregates (on quarterly rows),
and trailing 3y / 5y EPS CAGR (on annual rows, propagated to all rows).

Cadence: weekly, in weekly_refresh.yml after transform_fundamentals.

Run as:  python -m transform_gold.build_fundamentals_wide
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from transform_gold.utils.duckdb_silver import duckdb_to_r2, silver_scan, overwrite_parquet

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

OUT_KEY = "gold/fact_fundamentals_wide/fact_fundamentals_wide.parquet"


JOIN_SQL = """
-- Defensive dedup on each silver source. Silver should already be 1 row per
-- (symbol, fiscal_date_ending, period_type) but isn't (as of 2026-06) — there
-- are 2x duplicates in all four fundamentals tables. Without this dedup the
-- 4-way FULL OUTER JOIN multiplies 2^4 = 16x. Keep the row with the latest
-- pull_date.
WITH inc AS (
    SELECT * FROM {inc}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, fiscal_date_ending, period_type
        ORDER BY pull_date DESC NULLS LAST
    ) = 1
),
bal AS (
    SELECT * FROM {bal}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, fiscal_date_ending, period_type
        ORDER BY pull_date DESC NULLS LAST
    ) = 1
),
cf AS (
    SELECT * FROM {cf}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, fiscal_date_ending, period_type
        ORDER BY pull_date DESC NULLS LAST
    ) = 1
),
ern AS (
    SELECT * FROM {ern}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, fiscal_date_ending, period_type
        ORDER BY pull_date DESC NULLS LAST
    ) = 1
)
SELECT
    COALESCE(inc.symbol, bal.symbol, cf.symbol, ern.symbol) AS symbol,
    COALESCE(inc.fiscal_date_ending, bal.fiscal_date_ending, cf.fiscal_date_ending, ern.fiscal_date_ending) AS fiscal_date_ending,
    COALESCE(inc.period_type, bal.period_type, cf.period_type, ern.period_type) AS period_type,
    COALESCE(inc.reported_currency, bal.reported_currency, cf.reported_currency) AS reported_currency,

    -- Income statement (note: AV's INCOME_STATEMENT does not return EPS;
    -- reported_eps comes via the EARNINGS endpoint joined as `ern` below)
    inc.total_revenue, inc.cost_of_revenue, inc.gross_profit,
    inc.operating_expenses, inc.operating_income,
    inc.ebit, inc.ebitda, inc.depreciation_amortization,
    inc.income_before_tax, inc.income_tax,
    inc.net_income, inc.net_income_continuing_ops,
    inc.r_and_d, inc.sga, inc.interest_expense,

    -- Balance sheet
    bal.total_assets, bal.total_liabilities, bal.total_equity,
    bal.cash_and_equivalents, bal.short_term_investments,
    bal.current_assets, bal.current_liabilities,
    bal.long_term_debt, bal.short_term_debt,
    bal.retained_earnings, bal.goodwill, bal.intangible_assets,

    -- Cash flow
    cf.operating_cashflow, cf.capex, cf.free_cash_flow,
    cf.dividend_payout, cf.repurchase_of_stock,
    cf.proceeds_from_debt, cf.repayment_of_debt,
    cf.investing_cashflow, cf.financing_cashflow, cf.change_in_cash,

    -- Earnings (per fiscal period)
    ern.reported_eps, ern.estimated_eps, ern.surprise, ern.surprise_pct, ern.report_date,

    GREATEST(
        COALESCE(inc.pull_date, DATE '1900-01-01'),
        COALESCE(bal.pull_date, DATE '1900-01-01'),
        COALESCE(cf.pull_date,  DATE '1900-01-01'),
        COALESCE(ern.pull_date, DATE '1900-01-01')
    ) AS pull_date
FROM inc
FULL OUTER JOIN bal USING (symbol, fiscal_date_ending, period_type)
FULL OUTER JOIN cf  USING (symbol, fiscal_date_ending, period_type)
FULL OUTER JOIN ern USING (symbol, fiscal_date_ending, period_type)
"""


def build_base(con, bucket: str) -> pd.DataFrame:
    sql = JOIN_SQL.format(
        inc=silver_scan(bucket, "fact_income_statement/*.parquet"),
        bal=silver_scan(bucket, "fact_balance_sheet/*.parquet"),
        cf=silver_scan(bucket, "fact_cash_flow/*.parquet"),
        ern=silver_scan(bucket, "fact_earnings/*.parquet"),
    )
    df = con.execute(sql).df()
    logger.info(f"Joined base rows: {len(df):,}")
    return df


def add_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """Margin, return, leverage, quality ratios. Null-safe division."""
    def safe_div(a, b):
        return a / b.where(b != 0)

    df = df.sort_values(["symbol", "period_type", "fiscal_date_ending"]).reset_index(drop=True)

    # Margins
    df["gross_margin"]      = safe_div(df["gross_profit"],     df["total_revenue"])
    df["operating_margin"]  = safe_div(df["operating_income"], df["total_revenue"])
    df["net_margin"]        = safe_div(df["net_income"],       df["total_revenue"])
    df["fcf_margin"]        = safe_div(df["free_cash_flow"],   df["total_revenue"])
    df["ebitda_margin"]     = safe_div(df["ebitda"],           df["total_revenue"])

    # Returns on capital
    df["roe"]               = safe_div(df["net_income"], df["total_equity"])
    df["roa"]               = safe_div(df["net_income"], df["total_assets"])
    invested_capital        = df["total_equity"].fillna(0) + df["long_term_debt"].fillna(0)
    df["roic"]              = safe_div(df["operating_income"], invested_capital.where(invested_capital != 0))

    # Leverage
    df["debt_to_equity"]    = safe_div(
        df["long_term_debt"].fillna(0) + df["short_term_debt"].fillna(0),
        df["total_equity"],
    )
    df["net_debt"]          = (
        df["long_term_debt"].fillna(0) + df["short_term_debt"].fillna(0)
        - df["cash_and_equivalents"].fillna(0) - df["short_term_investments"].fillna(0)
    )
    df["interest_coverage"] = safe_div(df["operating_income"], df["interest_expense"])

    # Quality
    df["cash_conversion"]   = safe_div(df["operating_cashflow"], df["net_income"])
    df["accruals_ratio"]    = safe_div(df["net_income"] - df["operating_cashflow"], df["total_assets"])

    return df


def add_growth(df: pd.DataFrame) -> pd.DataFrame:
    """YoY (4 quarters back on quarterly, 1 year back on annual) and QoQ growth."""
    df = df.sort_values(["symbol", "period_type", "fiscal_date_ending"]).reset_index(drop=True)

    def yoy_lag(g):
        # 4 periods back if quarterly, 1 period back if annual
        lag_n = 4 if g.name[1] == "quarterly" else 1
        out = pd.DataFrame(index=g.index)
        for col in ["total_revenue", "net_income", "reported_eps", "free_cash_flow", "operating_cashflow"]:
            prev = g[col].shift(lag_n)
            out[f"{col}_yoy"] = (g[col] - prev) / prev.where(prev != 0)
        return out

    grp = df.groupby(["symbol", "period_type"], group_keys=False)
    yoy = grp.apply(yoy_lag)
    df["revenue_growth_yoy"]       = yoy["total_revenue_yoy"]
    df["net_income_growth_yoy"]    = yoy["net_income_yoy"]
    df["eps_growth_yoy"]           = yoy["reported_eps_yoy"]
    df["fcf_growth_yoy"]           = yoy["free_cash_flow_yoy"]
    df["operating_cf_growth_yoy"]  = yoy["operating_cashflow_yoy"]

    # QoQ only meaningful on quarterly
    def qoq(g):
        prev = g["total_revenue"].shift(1)
        return (g["total_revenue"] - prev) / prev.where(prev != 0)

    df["revenue_growth_qoq"] = (
        df.groupby(["symbol", "period_type"], group_keys=False)
          .apply(lambda g: qoq(g) if g.name[1] == "quarterly" else pd.Series(index=g.index, dtype=float))
    )

    return df


def add_ttm(df: pd.DataFrame) -> pd.DataFrame:
    """Trailing 12-month aggregates on quarterly rows (rolling sum of last 4 quarters).
    Null for annual rows."""
    df = df.sort_values(["symbol", "period_type", "fiscal_date_ending"]).reset_index(drop=True)

    ttm_cols = ["total_revenue", "net_income", "operating_income", "ebitda",
                "free_cash_flow", "operating_cashflow", "reported_eps", "capex",
                "dividend_payout"]

    q_mask = df["period_type"] == "quarterly"
    qdf = df[q_mask].copy()

    for col in ttm_cols:
        rolled = (
            qdf.groupby("symbol")[col]
               .rolling(window=4, min_periods=4).sum()
               .reset_index(level=0, drop=True)
        )
        df.loc[q_mask, f"{col}_ttm"] = rolled

    return df


def add_eps_cagr(df: pd.DataFrame) -> pd.DataFrame:
    """Trailing 5y and 3y EPS CAGR computed on annual rows; propagated to all rows
    for the same symbol.

    Source: `reported_eps` from fact_earnings. AV's INCOME_STATEMENT endpoint
    does not return EPS at all (confirmed across multiple tickers), so silver
    fact_income_statement intentionally does not carry an EPS column. The
    actually-reported figure from the EARNINGS endpoint is the canonical source.

    CAGR is undefined when either endpoint EPS is null or non-positive — null out.
    """
    annual = df[df["period_type"] == "annual"].copy()
    annual = annual.sort_values(["symbol", "fiscal_date_ending"]).reset_index(drop=True)

    eps = annual["reported_eps"].astype("float64")

    def cagr(end, start, years):
        valid = (start > 0) & (end > 0) & start.notna() & end.notna()
        out = pd.Series(index=end.index, dtype="float64")
        out[valid] = (end[valid] / start[valid]) ** (1.0 / years) - 1.0
        return out

    annual["eps_5y_ago"] = annual.groupby("symbol")["reported_eps"].shift(5).astype("float64")
    annual["eps_3y_ago"] = annual.groupby("symbol")["reported_eps"].shift(3).astype("float64")
    annual["eps_cagr_5y"] = cagr(eps, annual["eps_5y_ago"], 5)
    annual["eps_cagr_3y"] = cagr(eps, annual["eps_3y_ago"], 3)

    # Keep latest CAGR per symbol (most recent annual report)
    latest_cagr = (
        annual.sort_values("fiscal_date_ending")
              .groupby("symbol")
              .agg(eps_cagr_5y_latest=("eps_cagr_5y", "last"),
                   eps_cagr_3y_latest=("eps_cagr_3y", "last"),
                   eps_cagr_as_of=("fiscal_date_ending", "last"))
              .reset_index()
    )

    df = df.merge(latest_cagr, on="symbol", how="left")
    df = df.rename(columns={"eps_cagr_5y_latest": "eps_cagr_5y",
                              "eps_cagr_3y_latest": "eps_cagr_3y"})
    return df


def main():
    bucket = os.environ["R2_BUCKET_NAME"]
    con = duckdb_to_r2()

    df = build_base(con, bucket)
    df = add_ratios(df)
    df = add_growth(df)
    df = add_ttm(df)
    df = add_eps_cagr(df)

    df["gold_built_utc"] = datetime.now(timezone.utc).isoformat()

    logger.info(
        f"Final shape: {df.shape}  "
        f"({df['symbol'].nunique()} symbols, "
        f"{(df['period_type'] == 'annual').sum()} annual rows, "
        f"{(df['period_type'] == 'quarterly').sum()} quarterly rows)"
    )

    overwrite_parquet(df, OUT_KEY)


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("build_fundamentals_wide"):
        main()
