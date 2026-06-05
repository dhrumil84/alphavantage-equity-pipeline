"""
Ingest broad-US-market ETF holdings as our investable universe anchor.

Source: SSGA SPTM (SPDR Portfolio S&P 1500 Composite Stock Market ETF).
  - Tracks the S&P Composite 1500 (S&P 500 + S&P 400 + S&P 600).
  - ~1,500 US-listed equities covering ~90% of US market cap.
  - Free, ungated xlsx download from ssga.com — refreshes daily.

Why not iShares ITOT? ITOT (~2,500 names) would be a slightly broader anchor,
but iShares now gates the holdings CSV behind a legal interstitial that
returns HTML even when content-type says text/csv. SPTM is the same altitude
("main investable US equities") and ungated, so we use it as the automated
default. To swap in ITOT later (manual CSV download), drop the file at
config/itot_holdings_manual.csv and add a parser branch here.

Output:
  - bronze/sptm_holdings/{pull_date}.xlsx in R2 (raw archive)
  - config/vti_universe.csv with columns: symbol,name,sector,weight
    (file is named vti_universe.csv for forward compatibility — the column
    contract is what consumers care about, not the source ETF.)
"""
import io
import logging
from datetime import datetime
from pathlib import Path

import requests
import pandas as pd

from ingestion.utils import r2_client

logger = logging.getLogger(__name__)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

SPTM_URL = (
    "https://www.ssga.com/library-content/products/fund-data/etfs/us/"
    "holdings-daily-us-en-sptm.xlsx"
)
USER_AGENT = "Mozilla/5.0 (alphavantage-equity-pipeline; research use)"


def _normalise_symbol(sym: str) -> str:
    # SSGA uses dots in tickers (BRK.B); AV uses dashes (BRK-B). Normalise.
    return sym.strip().upper().replace(".", "-")


def _download() -> bytes:
    logger.info("Downloading SPTM holdings from SSGA...")
    resp = requests.get(SPTM_URL, headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()
    # Sanity check: real xlsx files start with PK (zip signature).
    if not resp.content.startswith(b"PK"):
        raise ValueError(
            f"SSGA returned non-xlsx content "
            f"(first 4 bytes: {resp.content[:4]!r}) — endpoint may have changed."
        )
    return resp.content


def parse_holdings(raw_bytes: bytes) -> pd.DataFrame:
    """
    SPTM xlsx layout (one sheet 'holdings'):
      rows 0-2: fund metadata (name, ticker, as-of date)
      row 3:    blank
      row 4:    column headers — Name, Ticker, Identifier, SEDOL, Weight,
                                 Sector, Shares Held, Local Currency
      row 5+:   holdings
    Footer rows after the last holding contain 'The fund's...' disclosure
    text; we filter those by requiring a non-null Ticker.
    """
    df = pd.read_excel(io.BytesIO(raw_bytes), sheet_name="holdings", header=4)
    df.columns = [c.strip() for c in df.columns]

    required = {"Name", "Ticker", "Weight"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"SPTM xlsx missing expected columns: {missing}")

    before = len(df)
    df = df[df["Ticker"].notna()].copy()
    df = df[df["Ticker"].astype(str).str.strip().str.len() > 0]
    df = df[~df["Ticker"].astype(str).str.startswith("The fund")]
    logger.info(f"Filtered to {len(df)} holdings rows (dropped {before - len(df)} metadata/footer)")

    out = pd.DataFrame({
        "symbol": df["Ticker"].astype(str).map(_normalise_symbol),
        "name": df["Name"].astype(str).str.strip(),
        "sector": df.get("Sector", pd.Series([""] * len(df))).astype(str).str.strip(),
        "weight": pd.to_numeric(df["Weight"], errors="coerce"),
    })
    out = out[out["symbol"].str.len() > 0]
    # Equity-symbol shape filter: only letters, optionally followed by a
    # single dash-letter share-class suffix (BRK-B, BF-B). Drops cash
    # placeholders ('-'), digit-prefixed CUSIP-like CVRs ('2200963D'), and
    # futures contracts ('ESM6'). This is what cleans up the residual
    # non-equity rows that SPTM occasionally includes.
    out = out[out["symbol"].str.fullmatch(r"[A-Z]+(-[A-Z])?")]
    out = out.drop_duplicates(subset=["symbol"]).sort_values("symbol").reset_index(drop=True)
    return out


def main() -> None:
    pull_date = datetime.now().strftime("%Y-%m-%d")
    r2_key = f"bronze/sptm_holdings/{pull_date}.xlsx"

    if r2_client.key_exists(r2_key):
        logger.info(f"{r2_key} already exists in R2 — using archived copy.")
        raw = r2_client.download_bytes(r2_key)
    else:
        raw = _download()
        logger.info(f"Archiving raw bytes to {r2_key} ({len(raw)} bytes)")
        r2_client.upload_bytes(raw, r2_key)

    df = parse_holdings(raw)
    logger.info(f"Parsed {len(df)} equity holdings. Top-5 weights:")
    for _, row in df.nlargest(5, "weight").iterrows():
        logger.info(f"  {row['symbol']:<8} {row['weight']:>6.2f}%  {row['name']}")

    out_path = Path("config") / "vti_universe.csv"
    out_path.parent.mkdir(exist_ok=True)
    df.to_csv(out_path, index=False)
    logger.info(f"Wrote {len(df)} rows to {out_path}")


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("ingest_sptm_holdings"):
        main()
