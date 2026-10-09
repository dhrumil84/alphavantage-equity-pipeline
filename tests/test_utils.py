"""Unit tests for the shared ingestion/transform utilities."""
from __future__ import annotations

import io
from datetime import date

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from ingestion.utils import av_client, freshness
from ingestion.utils.rate_limiter import RateLimiter
from transform.utils.parquet_writer import upsert_parquet


def read_parquet(fake_r2, key: str) -> pa.Table:
    return pq.read_table(io.BytesIO(fake_r2.download_bytes(key)))


# ─── parquet_writer.upsert_parquet ─────────────────────────────────────────

KEY = "silver/test_table/test_table.parquet"


def _rows(pull_date: str, close_by_date: dict[str, float]) -> pd.DataFrame:
    return pd.DataFrame([
        {"symbol": "AAPL", "trade_date": d, "close": c, "pull_date": pull_date}
        for d, c in close_by_date.items()
    ])


def test_upsert_creates_file_with_date32_columns(fake_r2):
    upsert_parquet(_rows("2026-01-06", {"2026-01-05": 1.0}), KEY, ["symbol", "trade_date"])
    table = read_parquet(fake_r2, KEY)
    assert table.num_rows == 1
    assert table.schema.field("trade_date").type == pa.date32()
    assert table.schema.field("pull_date").type == pa.date32()


def test_upsert_is_idempotent(fake_r2):
    df = _rows("2026-01-06", {"2026-01-05": 1.0, "2026-01-06": 2.0})
    upsert_parquet(df, KEY, ["symbol", "trade_date"])
    upsert_parquet(df, KEY, ["symbol", "trade_date"])
    assert read_parquet(fake_r2, KEY).num_rows == 2


def test_upsert_latest_pull_date_wins(fake_r2):
    upsert_parquet(_rows("2026-01-06", {"2026-01-05": 1.0}), KEY, ["symbol", "trade_date"])
    upsert_parquet(_rows("2026-01-07", {"2026-01-05": 9.0, "2026-01-07": 3.0}),
                   KEY, ["symbol", "trade_date"])
    df = read_parquet(fake_r2, KEY).to_pandas().set_index("trade_date")
    assert len(df) == 2
    assert df.loc[date(2026, 1, 5), "close"] == 9.0


def test_upsert_older_pull_does_not_overwrite_newer(fake_r2):
    upsert_parquet(_rows("2026-01-07", {"2026-01-05": 9.0}), KEY, ["symbol", "trade_date"])
    upsert_parquet(_rows("2026-01-06", {"2026-01-05": 1.0}), KEY, ["symbol", "trade_date"])
    df = read_parquet(fake_r2, KEY).to_pandas()
    assert df["close"].tolist() == [9.0]


# ─── freshness ─────────────────────────────────────────────────────────────

def test_jitter_is_deterministic_and_bounded():
    for ttl in (25, 60, 75):
        max_j = round(ttl * freshness.JITTER_FRACTION)
        for sym in ("AAPL", "MSFT", "BRK.B", "ZZZZ"):
            j = freshness.symbol_jitter_days(sym, ttl)
            assert j == freshness.symbol_jitter_days(sym, ttl)
            assert -max_j <= j <= max_j


def test_short_ttl_has_no_jitter():
    assert freshness.symbol_jitter_days("AAPL", 6) == 0


def test_build_fresh_symbol_set(fake_r2):
    today = date(2026, 3, 1)
    fake_r2.upload_json({}, "bronze/dividends/AAPL/2026-02-27.json")   # 2d old
    fake_r2.upload_json({}, "bronze/dividends/MSFT/2026-02-01.json")   # 28d old
    fake_r2.upload_json({}, "bronze/dividends/MSFT/not-a-date.json")   # ignored
    fresh = freshness.build_fresh_symbol_set("dividends", ttl_days=6, today=today)
    assert fresh == {"AAPL"}


# ─── rate_limiter ──────────────────────────────────────────────────────────

class FakeClock:
    def __init__(self):
        self.now = 1000.0
        self.slept: list[float] = []

    def time(self):
        return self.now

    def sleep(self, s):
        self.slept.append(s)
        self.now += s


def test_rate_limiter_blocks_only_after_capacity(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr("ingestion.utils.rate_limiter.time", clock)
    limiter = RateLimiter(calls_per_minute=3, window_seconds=60)
    for _ in range(3):
        limiter.wait()
    assert clock.slept == []
    limiter.wait()
    assert clock.slept == [pytest.approx(60.0)]


# ─── av_client ─────────────────────────────────────────────────────────────

class FakeResponse:
    def __init__(self, status=200, payload=None, text=""):
        self.status_code = status
        self._payload = payload
        self.text = text
        self.headers = {"Content-Type": "application/json"}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if 400 <= self.status_code:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")


@pytest.fixture
def av_env(monkeypatch):
    monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test-key")
    monkeypatch.setattr(av_client.time, "sleep", lambda s: None)


def _serve(monkeypatch, *responses):
    calls = []
    queue = list(responses)

    def fake_get(url, params=None, timeout=None):
        calls.append(params)
        return queue.pop(0)
    monkeypatch.setattr(av_client.requests, "get", fake_get)
    return calls


def test_fetch_returns_payload_and_adds_apikey(monkeypatch, av_env):
    calls = _serve(monkeypatch, FakeResponse(payload={"Time Series (Daily)": {}}))
    params = {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": "AAPL"}
    assert av_client.fetch(params) == {"Time Series (Daily)": {}}
    assert calls[0]["apikey"] == "test-key"
    assert "apikey" not in params  # caller's dict not mutated


@pytest.mark.parametrize("payload", [
    {"Error Message": "Invalid API call."},
    {"Information": "Thank you for using Alpha Vantage! premium endpoint"},
    {"Note": "Our standard API call frequency is 75 calls per minute."},
])
def test_fetch_raises_on_200_error_payloads(monkeypatch, av_env, payload):
    _serve(monkeypatch, FakeResponse(payload=payload))
    with pytest.raises(av_client.AlphaVantageError):
        av_client.fetch({"function": "OVERVIEW", "symbol": "AAPL"})


def test_fetch_retries_5xx_then_succeeds(monkeypatch, av_env):
    calls = _serve(monkeypatch,
                   FakeResponse(status=503, text="unavailable"),
                   FakeResponse(payload={"ok": True}))
    assert av_client.fetch({"function": "OVERVIEW"}) == {"ok": True}
    assert len(calls) == 2


def test_fetch_does_not_retry_4xx(monkeypatch, av_env):
    calls = _serve(monkeypatch, FakeResponse(status=404, text="nope"),
                   FakeResponse(payload={"ok": True}))
    with pytest.raises(av_client.AlphaVantageHTTPError):
        av_client.fetch({"function": "OVERVIEW"})
    assert len(calls) == 1


def test_fetch_requires_api_key(monkeypatch):
    monkeypatch.delenv("ALPHAVANTAGE_API_KEY", raising=False)
    with pytest.raises(ValueError):
        av_client.fetch({"function": "OVERVIEW"})
