"""Bronze → silver tests for transform_etf_profile on fixture data."""
from __future__ import annotations

import io
from datetime import date

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from transform import transform_etf_profile


def read_silver(fake_r2, table: str) -> pa.Table:
    key = f"silver/{table}/{table}.parquet"
    return pq.read_table(io.BytesIO(fake_r2.download_bytes(key)))


@pytest.fixture
def seeded(fake_r2):
    fake_r2.seed_fixture("etf_profile_SPY.json", "bronze/etf_profile/SPY/2026-11-15.json")
    # AV returns "n/a" for every field on some thinly covered ETFs (weekly run #34).
    fake_r2.seed_fixture("etf_profile_NEWETF.json", "bronze/etf_profile/NEWETF/2026-11-15.json")
    return fake_r2


def test_dim_etf_profile_schema_and_nulls(seeded):
    transform_etf_profile.main()
    table = read_silver(seeded, "dim_etf_profile")
    assert table.schema.field("inception_date").type == pa.date32()
    assert table.schema.field("pull_date").type == pa.date32()

    df = table.to_pandas().set_index("symbol")
    assert df.loc["SPY", "inception_date"] == date(1993, 1, 22)
    assert df.loc["SPY", "leveraged"] == "NO"
    assert df.loc["SPY", "net_assets"] == 632_000_000_000

    new = df.loc["NEWETF"]
    assert pd.isna(new.inception_date)  # "n/a" → NULL, not a crash
    assert pd.isna(new.leveraged)  # no "n/a" sentinel strings in silver
    assert pd.isna(new.net_assets) and pd.isna(new.net_expense_ratio)


def test_holdings_and_sectors(seeded):
    transform_etf_profile.main()
    holdings = read_silver(seeded, "fact_etf_holdings").to_pandas()
    assert set(holdings.etf_symbol) == {"SPY"}
    assert not holdings.duplicated(["etf_symbol", "holding_symbol", "as_of_date"]).any()
    assert (holdings.as_of_date == date(2026, 11, 15)).all()

    sectors = read_silver(seeded, "fact_etf_sector_allocation").to_pandas()
    assert len(sectors) == 2
    assert not sectors.duplicated(["etf_symbol", "sector", "as_of_date"]).any()


def test_etf_profile_transform_is_idempotent(seeded):
    tables = ("dim_etf_profile", "fact_etf_holdings", "fact_etf_sector_allocation")
    transform_etf_profile.main()
    first = {t: read_silver(seeded, t).to_pandas() for t in tables}
    transform_etf_profile.main()
    for t, df in first.items():
        pd.testing.assert_frame_equal(df, read_silver(seeded, t).to_pandas())


def test_safe_date():
    assert transform_etf_profile._safe_date("2020-05-01") == date(2020, 5, 1)
    for missing in (None, "", "n/a", "None", "garbage"):
        assert transform_etf_profile._safe_date(missing) is None
