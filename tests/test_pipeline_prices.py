"""
End-to-end tests for the daily price path on fixture data:

    ingest_daily_prices  →  bronze/daily_prices/{symbol}/{pull_date}.json
    transform_daily_prices → silver/fact_daily_prices/year=YYYY/...
    build_prices_enriched  → gold/fact_prices_enriched/year=YYYY/...
"""
from __future__ import annotations

import io
import sys
from datetime import date, datetime

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion import ingest_daily_prices
from ingestion.utils import av_client
from transform import transform_daily_prices
from transform_gold import build_prices_enriched

SILVER_PREFIX = "silver/fact_daily_prices/"


def read_parquet(fake_r2, key: str) -> pd.DataFrame:
    return pq.read_table(io.BytesIO(fake_r2.download_bytes(key))).to_pandas()


def read_silver_prices(fake_r2) -> pd.DataFrame:
    keys = fake_r2.list_keys(SILVER_PREFIX)
    return pd.concat([read_parquet(fake_r2, k) for k in keys], ignore_index=True)


def seed_bronze_prices(fake_r2, pull_date="2026-01-06"):
    for sym in ("AAPL", "MSFT"):
        fake_r2.seed_fixture(f"daily_prices_{sym}_2026-01-06.json",
                             f"bronze/daily_prices/{sym}/{pull_date}.json")


# ─── Bronze: ingestion ─────────────────────────────────────────────────────

@pytest.fixture
def run_ingest(monkeypatch, tests_fixture_payload):
    """Run ingest_daily_prices.main() in incremental mode against a fake API."""
    calls: list[str] = []

    def fake_fetch(params):
        calls.append(params["symbol"])
        return tests_fixture_payload

    monkeypatch.setattr(av_client, "fetch", fake_fetch)
    monkeypatch.setattr(ingest_daily_prices, "load_active_tickers",
                        lambda path: ["AAPL", "MSFT"])
    monkeypatch.setattr(sys, "argv", ["ingest_daily_prices", "--mode", "incremental"])

    def _run():
        ingest_daily_prices.main()
        return calls
    return _run


@pytest.fixture
def tests_fixture_payload(load_fixture):
    return load_fixture("daily_prices_AAPL_2026-01-06.json")


def test_ingest_writes_raw_response_to_bronze(fake_r2, run_ingest, tests_fixture_payload):
    calls = run_ingest()
    today = datetime.now().strftime("%Y-%m-%d")
    assert calls == ["AAPL", "MSFT"]
    key = f"bronze/daily_prices/AAPL/{today}.json"
    assert fake_r2.download_json(key) == tests_fixture_payload  # verbatim


def test_ingest_incremental_skips_existing_and_never_rewrites_bronze(fake_r2, run_ingest):
    run_ingest()
    writes_after_first = list(fake_r2.writes)
    calls = run_ingest()
    assert calls == ["AAPL", "MSFT"]  # second run made no new API calls
    assert fake_r2.writes == writes_after_first  # and wrote nothing


# ─── Silver: transform ─────────────────────────────────────────────────────

def test_transform_produces_typed_year_partitioned_silver(fake_r2, ticker_config):
    seed_bronze_prices(fake_r2)
    transform_daily_prices.main()

    assert fake_r2.list_keys(SILVER_PREFIX) == [
        "silver/fact_daily_prices/year=2025/fact_daily_prices.parquet",
        "silver/fact_daily_prices/year=2026/fact_daily_prices.parquet",
    ]
    table = pq.read_table(io.BytesIO(fake_r2.download_bytes(
        "silver/fact_daily_prices/year=2026/fact_daily_prices.parquet")))
    assert table.schema.field("trade_date").type == pa.date32()

    df = read_silver_prices(fake_r2)
    assert len(df) == 8  # 5 AAPL + 3 MSFT
    assert not df.duplicated(["symbol", "trade_date"]).any()

    aapl = df[df.symbol == "AAPL"].set_index("trade_date")
    assert aapl.loc[date(2025, 12, 30), "dividend_amount"] == pytest.approx(0.26)
    assert aapl.loc[date(2025, 12, 30), "adjusted_close"] == pytest.approx(252.44)


def test_transform_maps_missing_values_to_null(fake_r2, ticker_config):
    seed_bronze_prices(fake_r2)
    transform_daily_prices.main()
    msft = read_silver_prices(fake_r2).query("symbol == 'MSFT'").set_index("trade_date")
    assert pd.isna(msft.loc[date(2026, 1, 5), "volume"])   # "None"
    assert pd.isna(msft.loc[date(2026, 1, 2), "low"])      # ""


def test_transform_skips_inactive_tickers(fake_r2, ticker_config):
    seed_bronze_prices(fake_r2)
    fake_r2.seed_fixture("daily_prices_AAPL_2026-01-06.json",
                         "bronze/daily_prices/OLD/2026-01-06.json")
    transform_daily_prices.main()
    assert "OLD" not in set(read_silver_prices(fake_r2).symbol)


def test_transform_is_idempotent(fake_r2, ticker_config):
    seed_bronze_prices(fake_r2)
    transform_daily_prices.main()
    first = read_silver_prices(fake_r2)
    transform_daily_prices.main()
    second = read_silver_prices(fake_r2)
    pd.testing.assert_frame_equal(first, second)


def test_transform_newer_pull_revises_and_extends(fake_r2, ticker_config, load_fixture):
    seed_bronze_prices(fake_r2)
    transform_daily_prices.main()

    # A later pull restates one close and adds a new trading day.
    payload = load_fixture("daily_prices_AAPL_2026-01-06.json")
    series = payload["Time Series (Daily)"]
    series["2026-01-06"]["4. close"] = "999.0000"
    series["2026-01-07"] = dict(series["2026-01-06"], **{"4. close": "254.0000"})
    fake_r2.upload_json(payload, "bronze/daily_prices/AAPL/2026-01-07.json")

    transform_daily_prices.main()
    aapl = read_silver_prices(fake_r2).query("symbol == 'AAPL'").set_index("trade_date")
    assert len(aapl) == 6
    assert aapl.loc[date(2026, 1, 6), "close"] == 999.0
    assert aapl.loc[date(2026, 1, 6), "pull_date"] == date(2026, 1, 7)


# ─── Gold: build_prices_enriched ──────────────────────────────────────────

def _synthetic_prices(symbol: str, n: int, start=100.0, step=1.0) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=n)
    ac = start + step * np.arange(n)
    return pd.DataFrame({
        "symbol": symbol, "trade_date": dates,
        "open": ac, "high": ac + 1, "low": ac - 1, "close": ac,
        "adjusted_close": ac, "volume": 1_000_000,
    })


def test_enrich_group_returns_and_moving_averages():
    g = build_prices_enriched.enrich_group(_synthetic_prices("AAPL", 260))
    # Linear price series: values are easy to verify by hand.
    assert g.loc[1, "return_1d"] == pytest.approx(101 / 100 - 1)
    assert g.loc[252, "return_252d"] == pytest.approx(352 / 100 - 1)
    assert g.loc[19, "sma_20"] == pytest.approx(np.mean(100 + np.arange(20)))
    assert pd.isna(g.loc[18, "sma_20"])          # min_periods respected
    assert g["pct_off_52w_high"].dropna().iloc[-1] == pytest.approx(0.0)


def test_enrich_group_rsi_bounded_on_mixed_series():
    # Note: on a series with zero down days, _rsi currently returns NaN rather
    # than the textbook 100 (avg_loss == 0 is masked). Not asserted here.
    prices = _synthetic_prices("AAPL", 60)
    prices["adjusted_close"] = 100 + np.where(np.arange(60) % 3 == 0, -1.0, 1.0).cumsum()
    rsi = build_prices_enriched.enrich_group(prices)["rsi_14"]
    assert rsi.iloc[:14].isna().all()            # warm-up period
    assert rsi.iloc[14:].between(0, 100).all()


def test_build_prices_enriched_end_to_end(fake_r2, ticker_config, monkeypatch):
    """Silver (from the real transform) → gold, read back through DuckDB."""
    seed_bronze_prices(fake_r2)
    transform_daily_prices.main()

    monkeypatch.setenv("R2_BUCKET_NAME", "test-bucket")
    monkeypatch.setattr(build_prices_enriched, "duckdb_to_r2", duckdb.connect)
    monkeypatch.setattr(
        build_prices_enriched, "silver_scan",
        lambda bucket, path: f"read_parquet('{fake_r2.root}/silver/{path}', union_by_name=true)",
    )
    build_prices_enriched.main()

    gold_keys = fake_r2.list_keys("gold/fact_prices_enriched/")
    assert gold_keys == [
        "gold/fact_prices_enriched/year=2025/fact_prices_enriched.parquet",
        "gold/fact_prices_enriched/year=2026/fact_prices_enriched.parquet",
    ]
    gold = pd.concat([read_parquet(fake_r2, k) for k in gold_keys], ignore_index=True)
    assert len(gold) == 8
    assert not gold.duplicated(["symbol", "trade_date"]).any()
    for col in ("return_1d", "sma_20", "rsi_14", "rel_strength_vs_spy_3m"):
        assert col in gold.columns

    aapl = gold[gold.symbol == "AAPL"].sort_values("trade_date").reset_index(drop=True)
    assert aapl.loc[1, "return_1d"] == pytest.approx(250.4 / 252.44 - 1)
