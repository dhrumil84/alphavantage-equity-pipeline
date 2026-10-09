"""Bronze → silver tests for transform_fundamentals on fixture data."""
from __future__ import annotations

import io
from datetime import date

import pandas as pd
import pyarrow.parquet as pq
import pytest

from transform import transform_fundamentals


def read_silver(fake_r2, table: str) -> pd.DataFrame:
    key = f"silver/{table}/{table}.parquet"
    return pq.read_table(io.BytesIO(fake_r2.download_bytes(key))).to_pandas()


@pytest.fixture
def seeded(fake_r2, ticker_config):
    for endpoint in ("income_statement", "cash_flow", "earnings"):
        fake_r2.seed_fixture(f"{endpoint}_AAPL.json",
                             f"bronze/{endpoint}/AAPL/2026-01-06.json")
    return fake_r2


def test_income_statement_silver(seeded):
    transform_fundamentals.main()
    df = read_silver(seeded, "fact_income_statement")
    assert len(df) == 4  # 2 annual + 2 quarterly
    assert set(df.period_type) == {"annual", "quarterly"}
    assert not df.duplicated(["symbol", "fiscal_date_ending", "period_type"]).any()

    fy25 = df[(df.period_type == "annual") & (df.fiscal_date_ending == date(2025, 9, 30))].iloc[0]
    assert fy25.total_revenue == 416_161_000_000
    assert pd.isna(fy25.interest_expense)  # "None" → NULL, not a sentinel


def test_cash_flow_free_cash_flow(seeded):
    """free_cash_flow is the one derived metric allowed in silver."""
    transform_fundamentals.main()
    df = read_silver(seeded, "fact_cash_flow").set_index("period_type")
    assert df.loc["annual", "free_cash_flow"] == 111_482_000_000 - 12_715_000_000
    # Null capex is treated as 0 for FCF but stays NULL in the capex column.
    assert pd.isna(df.loc["quarterly", "capex"])
    assert df.loc["quarterly", "free_cash_flow"] == 29_728_000_000


def test_earnings_drops_spurious_ttm_annual_row(seeded):
    transform_fundamentals.main()
    df = read_silver(seeded, "fact_earnings")
    annual = df[df.period_type == "annual"]
    assert sorted(annual.fiscal_date_ending) == [
        date(2023, 9, 30), date(2024, 9, 30), date(2025, 9, 30)]
    q = df[df.period_type == "quarterly"].set_index("fiscal_date_ending")
    assert q.loc[date(2025, 9, 30), "report_date"] == date(2025, 10, 30)
    assert pd.isna(q.loc[date(2025, 6, 30), "estimated_eps"])


def test_fundamentals_transform_is_idempotent(seeded):
    transform_fundamentals.main()
    first = {t: read_silver(seeded, t) for t in
             ("fact_income_statement", "fact_cash_flow", "fact_earnings")}
    transform_fundamentals.main()
    for table, df in first.items():
        pd.testing.assert_frame_equal(df, read_silver(seeded, table))


def test_filter_real_annual_earnings_keeps_short_lists():
    rows = [{"fiscalDateEnding": "2025-12-31"}, {"fiscalDateEnding": "2025-09-30"}]
    assert transform_fundamentals.filter_real_annual_earnings(rows) == rows
    assert transform_fundamentals.filter_real_annual_earnings([]) == []
