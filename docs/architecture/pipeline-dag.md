# Pipeline DAG

Job dependencies for the two GitHub Actions workflows. Source of truth is `.github/workflows/*.yml`; this diagram should be updated whenever a workflow step is added, removed, or reordered.

## Daily workflow

Trigger: cron `0 23 * * 1-5` (6pm ET, weekdays) — see [daily_prices.yml](../../.github/workflows/daily_prices.yml).

```mermaid
flowchart TB
    A[ingest_daily_prices<br/>--mode incremental] --> B[transform_daily_prices<br/>bronze → silver]
    B --> C[build_prices_enriched<br/>silver → gold]
    C --> D[build_valuation_daily<br/>uses fundamentals_wide snapshot]
    D --> E[quality.runner --suite daily]
    E --> F[observability.storage_scan]
    F -. always runs .- F
```

Notes:
- `build_valuation_daily` depends on `gold/fact_fundamentals_wide`, which is refreshed in the **weekly** run. The daily run reads whichever snapshot is current.
- `storage_scan` runs with `if: always()` so we still get metrics on a failed pipeline.

## Weekly workflow

Trigger: cron `0 6 * * 0` (Sunday 06:00 UTC) — see [weekly_refresh.yml](../../.github/workflows/weekly_refresh.yml).

```mermaid
flowchart TB
    subgraph Ingest
        I1[ingest_listing_status]
        I2[ingest_overview]
        I3[ingest_fundamentals]
        I4[ingest_corporate_actions]
        I1 --> I2 --> I3 --> I4
    end

    subgraph Transform_Silver
        T1[transform_overview]
        T2[transform_fundamentals]
        T3[transform_corporate_actions]
        T1 --> T2 --> T3
    end

    subgraph Transform_Gold
        G1[build_fundamentals_wide]
        G2[build_dim_company_enriched]
        G3[build_valuation_daily]
        G4[build_sector_aggregates]
        G5[build_peer_relative]
        G1 --> G2
        G2 --> G3
        G3 --> G4
        G4 --> G5
    end

    Q[quality.runner --suite weekly]
    O[observability.storage_scan]

    Ingest --> Transform_Silver --> Transform_Gold --> Q --> O
```

Notes:
- Gold ordering is load-bearing — comments in [weekly_refresh.yml](../../.github/workflows/weekly_refresh.yml) explain why each step reads from the prior one.
- Ingest steps are sequential today (single runner, shared rate limiter). If we ever shard tickers, this is where the diagram changes first.
