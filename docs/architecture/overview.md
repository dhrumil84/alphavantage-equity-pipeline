# System Overview

High-level view of the equity data pipeline. For schemas see [data-model.md](data-model.md); for job order see [pipeline-dag.md](pipeline-dag.md).

## C4 Level 1 — Context

Who and what interacts with the system.

```mermaid
flowchart LR
    User([Dhrumil<br/>analyst / developer])
    AV[Alpha Vantage API<br/>75 calls/min]
    GHA[GitHub Actions<br/>cron]
    System[[Equity Data Pipeline]]
    Notebooks[DuckDB notebooks<br/>backtests / screens]

    GHA -->|triggers| System
    System -->|HTTP GET| AV
    AV -->|JSON| System
    User -->|writes / queries| Notebooks
    Notebooks -->|reads Parquet| System
    User -->|maintains| System
```

## C4 Level 2 — Containers / Layers

The bronze → silver → gold lakehouse pattern, all backed by Cloudflare R2.

```mermaid
flowchart TB
    subgraph Source
        AV[Alpha Vantage API]
    end

    subgraph Runners["GitHub Actions runners"]
        ING[ingestion/*<br/>Python]
        TS[transform/*<br/>silver build]
        TG[transform_gold/*<br/>gold build]
        QC[quality/runner<br/>DQ checks]
        OBS[observability/<br/>storage_scan]
    end

    subgraph R2["Cloudflare R2 — equity-data-lake"]
        BRONZE[(bronze/<br/>raw JSON, immutable<br/>partitioned by symbol & pull_date)]
        SILVER[(silver/<br/>typed Parquet<br/>cleaned, deduped)]
        GOLD[(gold/<br/>derived facts + dims<br/>fundamentals_wide, valuation_daily,<br/>sector_aggregates, peer_relative)]
        REPORTS[(reports/<br/>DQ + storage metrics)]
    end

    Consumers[DuckDB notebooks<br/>backtests / screens]

    AV -->|HTTP| ING
    ING -->|write once| BRONZE
    BRONZE -->|read| TS
    TS -->|upsert| SILVER
    SILVER -->|read| TG
    TG -->|write| GOLD
    SILVER -->|read| QC
    GOLD -->|read| QC
    QC -->|write| REPORTS
    OBS -->|scan| R2
    OBS -->|write| REPORTS

    GOLD -->|httpfs Parquet| Consumers
    SILVER -->|httpfs Parquet| Consumers
```

## Design principles (one-liners)

1. **Bronze is immutable.** Raw API payloads are never rewritten. See [PROJECT_BRIEF.md](../../PROJECT_BRIEF.md).
2. **Silver is the analytical source of truth for source data.** Typed, deduped, no string `"None"`. No derived metrics (one exception: `free_cash_flow`).
3. **Gold holds derived facts.** Ratios, returns, peer/sector comparisons. Recomputable from silver.
4. **Idempotency everywhere.** Every job is safe to re-run; transforms upsert on a documented key.
5. **Rate-limit shared.** All API calls flow through `ingestion/utils/rate_limiter.py`.
6. **Universe by config.** Scope of work driven by `config/ticker_universe.csv`, not code.
