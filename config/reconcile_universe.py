"""
Reconcile the ITOT-anchored VTI universe against AV's latest listing_status.

Outputs three CSVs under config/reconcile/ for review:
  - matched.csv:       symbols in ITOT that AV lists as active stocks (the
                       ingestable universe for the next step-up).
  - mismatch.csv:      symbols where AV uses a different separator or form
                       (e.g. BRK-B vs BRKB), resolved via a fallback lookup.
                       These can be added with their AV symbol.
  - gaps.csv:          ITOT symbols AV does not list as active at all.
                       Spot-check: usually share-class oddities, recent IPOs,
                       or genuine AV coverage holes.

Also prints a coverage summary to stdout.
"""
import io
import logging
from pathlib import Path

import pandas as pd

from ingestion.utils import r2_client

logger = logging.getLogger(__name__)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)


def _latest_listing_status_key() -> str:
    keys = r2_client.list_keys("bronze/listing_status/")
    dated = []
    for k in keys:
        name = k.split("/")[-1]
        # Accept both '{date}.csv' (current combined format) and legacy
        # '{date}_active.csv'. We prefer the most recent combined file.
        stem = name.split(".")[0].split("_")[0]
        try:
            pd.to_datetime(stem)
            dated.append((stem, k))
        except (ValueError, TypeError):
            continue
    if not dated:
        raise FileNotFoundError("No listing_status pulls found in R2")
    dated.sort()
    return dated[-1][1]


def _load_listing_status() -> pd.DataFrame:
    key = _latest_listing_status_key()
    logger.info(f"Loading listing status from {key}")
    raw = r2_client.download_bytes(key)
    df = pd.read_csv(io.BytesIO(raw))
    df.columns = [c.strip() for c in df.columns]
    df = df[df["symbol"].notna()].copy()
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["assetType"] = df["assetType"].astype(str).str.strip()
    df["status"] = df["status"].astype(str).str.strip().str.lower()
    df = df[df["symbol"].str.len() > 0]
    return df


def _build_fuzzy_lookup(av: pd.DataFrame) -> dict:
    """
    Build a lookup from 'alphanumeric-only' symbol to actual AV symbol, so we
    can catch separator mismatches (BRKB <-> BRK-B, BFB <-> BF-B, etc.).
    Restricted to active stocks to avoid pulling in random preferreds.
    """
    active_stocks = av[(av["status"] == "active") & (av["assetType"] == "Stock")]
    fuzzy = {}
    for sym in active_stocks["symbol"]:
        flat = "".join(c for c in sym if c.isalnum())
        # Only register the fuzzy key if it doesn't already collide. A collision
        # means the flattening is ambiguous and we shouldn't auto-resolve it.
        if flat in fuzzy:
            fuzzy[flat] = None  # mark ambiguous
        else:
            fuzzy[flat] = sym
    return {k: v for k, v in fuzzy.items() if v is not None}


def main() -> None:
    vti_path = Path("config") / "vti_universe.csv"
    if not vti_path.exists():
        raise FileNotFoundError(
            f"{vti_path} not found. Run `python -m ingestion.ingest_itot_holdings` first."
        )

    vti = pd.read_csv(vti_path)
    vti["symbol"] = vti["symbol"].astype(str).str.upper()
    logger.info(f"Loaded {len(vti)} ITOT/VTI symbols")

    av = _load_listing_status()
    active_stocks = av[(av["status"] == "active") & (av["assetType"] == "Stock")]
    logger.info(
        f"AV listing_status: {len(av)} total rows, "
        f"{len(active_stocks)} active stocks"
    )

    av_active_symbols = set(active_stocks["symbol"])
    fuzzy = _build_fuzzy_lookup(av)

    matched, mismatch, gaps = [], [], []
    for _, row in vti.iterrows():
        s = row["symbol"]
        if s in av_active_symbols:
            matched.append({**row.to_dict(), "av_symbol": s})
            continue
        flat = "".join(c for c in s if c.isalnum())
        alt = fuzzy.get(flat)
        if alt and alt != s:
            mismatch.append({**row.to_dict(), "av_symbol": alt})
        else:
            gaps.append(row.to_dict())

    out_dir = Path("config") / "reconcile"
    out_dir.mkdir(exist_ok=True)
    pd.DataFrame(matched).to_csv(out_dir / "matched.csv", index=False)
    pd.DataFrame(mismatch).to_csv(out_dir / "mismatch.csv", index=False)
    pd.DataFrame(gaps).to_csv(out_dir / "gaps.csv", index=False)

    total = len(vti)
    print()
    print(f"=== Reconciliation summary ===")
    print(f"  ITOT universe:        {total:>5}")
    print(f"  Matched directly:     {len(matched):>5}  ({100*len(matched)/total:.1f}%)")
    print(f"  Separator mismatch:   {len(mismatch):>5}  (auto-resolvable)")
    print(f"  Genuine gaps:         {len(gaps):>5}  ({100*len(gaps)/total:.1f}%)")
    print()
    print(f"Wrote {out_dir}/matched.csv, mismatch.csv, gaps.csv")

    if mismatch:
        print()
        print("Sample mismatches (first 10):")
        for r in mismatch[:10]:
            print(f"  ITOT {r['symbol']:<8} -> AV {r['av_symbol']:<8}  {r['name']}")
    if gaps:
        print()
        print("Sample gaps (first 10):")
        for r in gaps[:10]:
            print(f"  {r['symbol']:<8} {r.get('name', '')}")


if __name__ == "__main__":
    main()
