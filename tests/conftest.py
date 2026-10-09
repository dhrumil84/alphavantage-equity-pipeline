"""
Shared pytest fixtures.

Every test runs against a fake, on-disk R2 and with outbound HTTP blocked, so
the suite needs no credentials and can never touch the real bucket or the
Alpha Vantage API — even if a developer has a populated .env locally.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
import requests

from ingestion.utils import r2_client

FIXTURES = Path(__file__).parent / "fixtures"


class FakeR2:
    """Stand-in for ingestion.utils.r2_client backed by a temp directory.

    Object keys map 1:1 to file paths under `root`, so DuckDB can read silver
    Parquet straight off disk in gold-layer tests.
    """

    def __init__(self, root: Path):
        self.root = root
        self.writes: list[str] = []

    def _path(self, key: str) -> Path:
        return self.root / key

    # -- r2_client API -----------------------------------------------------
    def upload_json(self, data: dict, key: str) -> None:
        self.upload_bytes(json.dumps(data).encode("utf-8"), key)

    def upload_bytes(self, data: bytes, key: str) -> None:
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        self.writes.append(key)

    def download_bytes(self, key: str) -> bytes:
        p = self._path(key)
        if not p.exists():
            raise FileNotFoundError(key)
        return p.read_bytes()

    def download_json(self, key: str) -> dict:
        return json.loads(self.download_bytes(key))

    def key_exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(
            p.relative_to(self.root).as_posix()
            for p in self.root.rglob("*")
            if p.is_file() and p.relative_to(self.root).as_posix().startswith(prefix)
        )

    # -- test helpers ------------------------------------------------------
    def seed_fixture(self, fixture_name: str, key: str) -> None:
        """Copy tests/fixtures/<fixture_name> into the fake bucket at `key`."""
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURES / fixture_name, p)


@pytest.fixture(autouse=True)
def fake_r2(tmp_path, monkeypatch) -> FakeR2:
    fake = FakeR2(tmp_path / "r2")
    fake.root.mkdir()
    for name in ("upload_json", "upload_bytes", "download_bytes",
                 "download_json", "key_exists", "list_keys"):
        monkeypatch.setattr(r2_client, name, getattr(fake, name))

    def _no_real_client():
        raise RuntimeError("Tests must not create a real R2 client")
    monkeypatch.setattr(r2_client, "_get_client", _no_real_client)
    return fake


@pytest.fixture(autouse=True)
def block_network(monkeypatch):
    """Fail loudly if any code path tries a real HTTP call."""
    def _blocked(*args, **kwargs):
        raise RuntimeError("Network access is blocked in tests")
    monkeypatch.setattr(requests, "get", _blocked)
    monkeypatch.setattr(requests, "post", _blocked)


@pytest.fixture
def ticker_config(tmp_path, monkeypatch):
    """Run from a temp cwd with a small config/ticker_universe.csv.

    Transforms read config/ticker_universe.csv relative to the cwd; this keeps
    tests independent of the real (frequently edited) universe file.
    """
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "ticker_universe.csv").write_text(
        "symbol,name,active\n"
        "AAPL,Apple,true\n"
        "MSFT,Microsoft,true\n"
        "OLD,Inactive Co,false\n"
    )
    monkeypatch.chdir(tmp_path)
    return cfg / "ticker_universe.csv"


@pytest.fixture
def load_fixture():
    """Return a fresh parsed copy of tests/fixtures/<name>."""
    return lambda name: json.loads((FIXTURES / name).read_text())
