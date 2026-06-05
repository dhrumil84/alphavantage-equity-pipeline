# Data Model

Entity-relationship view of the silver and gold tables. Authoritative column-level schemas live in [DATA_MODEL.md](../../DATA_MODEL.md); this page is for the relationships and grain at a glance.

## Silver layer

Cleaned, typed source data. One physical Parquet table (or partitioned set) per logical entity.

```mermaid
erDiagram
    dim_company ||--o{ fact_daily_prices : "symbol"
    dim_company ||--o{ fact_income_statement : "symbol"
    dim_company ||--o{ fact_balance_sheet : "symbol"
    dim_company ||--o{ fact_cash_flow : "symbol"
    dim_company ||--o{ fact_earnings : "symbol"
    dim_company ||--o{ fact_dividends : "symbol"
    dim_company ||--o{ fact_splits : "symbol"

    dim_company {
        string symbol PK
        string name
        string exchange
        string sector
        string industry
        string listing_status
        date   ipo_date
        date   delisted_date
        bigint market_cap
        bigint shares_outstanding
    }
    fact_daily_prices {
        string  symbol PK
        date    trade_date PK
        decimal adjusted_close
        bigint  volume
    }
    fact_income_statement {
        string symbol PK
        date   fiscal_date_ending PK
        string period_type PK
        bigint total_revenue
        bigint net_income
    }
    fact_balance_sheet {
        string symbol PK
        date   fiscal_date_ending PK
        string period_type PK
        bigint total_assets
        bigint total_equity
    }
    fact_cash_flow {
        string symbol PK
        date   fiscal_date_ending PK
        string period_type PK
        bigint operating_cashflow
        bigint free_cash_flow
    }
    fact_earnings {
        string  symbol PK
        date    fiscal_date_ending PK
        string  period_type PK
        decimal reported_eps
        date    report_date
    }
    fact_dividends {
        string  symbol PK
        date    ex_date PK
        decimal amount
    }
    fact_splits {
        string  symbol PK
        date    effective_date PK
        decimal split_factor
    }
```

## Gold layer

Derived facts built by `transform_gold/*`. Recomputable; can be dropped and rebuilt.

```mermaid
erDiagram
    dim_company_enriched ||--o{ fact_prices_enriched : "symbol"
    dim_company_enriched ||--o{ fact_valuation_daily : "symbol"
    dim_company_enriched ||--o{ fact_fundamentals_wide : "symbol"
    fact_fundamentals_wide ||--o{ fact_valuation_daily : "symbol + period"
    fact_prices_enriched ||--o{ fact_valuation_daily : "symbol + date"
    fact_valuation_daily ||--o{ fact_sector_aggregates : "sector + date"
    fact_valuation_daily ||--o{ fact_peer_relative : "symbol + date"
    fact_sector_aggregates ||--o{ fact_peer_relative : "sector + date"

    dim_company_enriched {
        string symbol PK
        string sector
        string industry
        string size_bucket
    }
    fact_prices_enriched {
        string  symbol PK
        date    trade_date PK
        decimal adjusted_close
        decimal return_1d
        decimal return_1m
    }
    fact_fundamentals_wide {
        string  symbol PK
        date    fiscal_date_ending PK
        string  period_type PK
        bigint  revenue
        bigint  net_income
        bigint  free_cash_flow
    }
    fact_valuation_daily {
        string  symbol PK
        date    trade_date PK
        decimal pe_ttm
        decimal ps_ttm
        decimal pfcf_ttm
        decimal ev_ebitda
    }
    fact_sector_aggregates {
        string  sector PK
        date    trade_date PK
        decimal median_pe
        decimal median_ps
    }
    fact_peer_relative {
        string  symbol PK
        date    trade_date PK
        decimal pe_vs_sector
        decimal ps_vs_sector
    }
```

> The ER diagrams above show *grain and keys only* — they are not exhaustive column lists. When schemas change, update [DATA_MODEL.md](../../DATA_MODEL.md) (authoritative) and refresh the keys here if the grain changed.
