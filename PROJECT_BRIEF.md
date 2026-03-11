# Equity Data Pipeline — Project Brief

## Purpose

This project builds a personal financial data lakehouse by ingesting data from the Alpha Vantage
API into a structured, analytics-ready storage layer. The goal is to support self-directed
analytical workflows: backtesting trading strategies, calculating portfolio returns across arbitrary
baskets of stocks, screening companies by fundamental criteria, and mining historical financial
statement data across a broad universe of US-listed equities.

This is also a learning exercise in modern data engineering practices and agentic development.

---

## Tech Stack (All Decisions Are Final — Do Not Substitute)

| Layer | Tool | Notes |
|---|---|---|
| API Source | Alpha Vantage (Premium key) | 75 calls/minute rate limit |
| Raw Storage | Cloudflare R2 | S3-compatible object storage |
| Analytics Engine | DuckDB | Local, reads Parquet from R2 directly |
| Orchestration | GitHub Actions | Cron-scheduled Python scripts |
| Language | Python 3.11+ | All ingestion and transformation scripts |
| IDE | Google Antigravity | Agentic development platform |

There is no application database (e.g. PostgreSQL). All persistent state lives in R2 as files.
There is no Spark, no Databricks, no cloud compute beyond GitHub Actions runners.

---

## Architecture Overview

```
GitHub Actions (cron schedule)
        │
        ▼
Python ingestion scripts
        │  HTTP calls (rate-limited to 75/min)
        ▼
Alpha Vantage API
        │  raw JSON responses
        ▼
Cloudflare R2  /bronze/       ← immutable raw layer
        │
        ▼
Python transformer scripts (also run via GitHub Actions)
        │  JSON → typed, cleaned Parquet
        ▼
Cloudflare R2  /silver/       ← analytics-ready Parquet tables
        │
        ▼
DuckDB (local)                ← ad-hoc SQL queries, notebooks, backtests
```

---

## Core Design Principles

### 1. Bronze is Immutable
Raw JSON files in `/bronze/` are never overwritten or deleted. Each pull creates a new file
timestamped by pull date. Bronze is your insurance policy: if the schema changes, if you made
a parsing mistake, if you want to reprocess anything — the original source data is always there.

### 2. Silver is the Single Source of Truth for Analysis
All analytical queries, DuckDB notebooks, and backtests read from `/silver/` only. Never query
bronze directly for analysis.

### 3. Idempotency Everywhere
Every script must be safe to run multiple times without creating duplicate data. Ingestion scripts
check what already exists before calling the API. Transformation scripts use upsert logic
(overwrite on matching keys) when writing Parquet.

### 4. Rate Limit Respect
All API calls must pass through a shared rate limiter enforcing the 75 calls/minute limit.
This is a utility shared by all ingestion scripts — it is never reimplemented per-script.

### 5. Secrets Stay Out of Code
API keys and R2 credentials are read exclusively from environment variables. They are never
hardcoded. In GitHub Actions they are stored as GitHub Secrets. Locally they are stored in a
`.env` file that is gitignored.

### 6. Start Small, Scale by Config
The ticker universe is controlled by a single config file: `config/ticker_universe.csv`.
Scripts read this file to determine which tickers to process. To expand the universe, update
this file. No code changes required.

---

## Ticker Universe Strategy

| Phase | Universe | Purpose |
|---|---|---|
| Phase 1 | 5–10 hand-picked tickers | Validate pipeline end-to-end |
| Phase 2 | S&P 500 (~500 tickers) | Validate at real scale |
| Phase 3 | Full US listed (~8,000 tickers) | Production historical backfill |

The `config/ticker_universe.csv` file has three columns: `symbol`, `name`, `active`.
Set `active=false` to exclude a ticker from processing without deleting it from the config.

---

## Endpoints in Scope (Phase 1)

### Core Price Data
- `TIME_SERIES_DAILY_ADJUSTED` — daily OHLCV + adjusted close + split/dividend events

### Fundamental Data
- `OVERVIEW` — company metadata, sector, market cap
- `INCOME_STATEMENT` — annual and quarterly P&L
- `BALANCE_SHEET` — annual and quarterly
- `CASH_FLOW` — annual and quarterly
- `EARNINGS` — EPS history, estimates vs actuals, report dates
- `DIVIDENDS` — corporate action history
- `SPLITS` — stock split history
- `SHARES_OUTSTANDING` — share count over time
- `LISTING_STATUS` — full universe of active and delisted tickers (no ticker loop — one call)
- `EARNINGS_CALENDAR` — forward-looking, stored bronze-only
- `IPO_CALENDAR` — forward-looking, stored bronze-only

### Out of Scope (Phase 1, extensible later)
- Options data
- Alpha Intelligence (insider transactions, earnings transcripts, news sentiment)
- Forex, crypto, commodities, economic indicators

---

## Incremental Load Strategy

After the initial historical backfill, scripts run on these schedules:

| Endpoint | Schedule | Logic |
|---|---|---|
| Daily prices | Daily, weekdays 6pm ET | Pull only tickers where max(trade_date) < today |
| Company overview | Weekly, Sunday | Full refresh for all active tickers |
| Income / Balance / Cash Flow | Weekly, Sunday | Pull full history, write only new fiscal periods to silver |
| Earnings | Weekly, Sunday | Same as above |
| Dividends / Splits | Weekly, Sunday | Pull full history, write only new events |
| Listing status | Weekly, Sunday | Detect new listings and delistings; update dim_company |
| Earnings / IPO Calendar | Weekly, Sunday | Overwrite bronze; no silver table |

---

## Handling Delistings (Survivorship Bias)

This is critical for backtest integrity.

- Bronze and silver records for delisted tickers are **never deleted**.
- `dim_company` has a `listing_status` column ('active' / 'delisted') and a `delisted_date` column.
- When `LISTING_STATUS` detects a ticker has been delisted, `dim_company` is updated but no
  historical data is removed.
- Backtest queries filter the universe using point-in-time logic:
  `WHERE ipo_date <= :analysis_date AND (delisted_date IS NULL OR delisted_date > :analysis_date)`

---

## Repository Structure (Target)

```
equity-pipeline/
├── config/
│   └── ticker_universe.csv          # ticker list — controls what gets processed
├── ingestion/
│   ├── utils/
│   │   ├── rate_limiter.py          # shared 75 calls/min rate limiter
│   │   ├── r2_client.py             # shared R2 read/write helpers
│   │   └── av_client.py             # shared Alpha Vantage HTTP client
│   ├── ingest_daily_prices.py
│   ├── ingest_fundamentals.py       # covers income, balance, cashflow, earnings
│   ├── ingest_overview.py
│   ├── ingest_corporate_actions.py  # dividends, splits, shares outstanding
│   └── ingest_listing_status.py
├── transform/
│   ├── utils/
│   │   └── parquet_writer.py        # shared upsert-to-parquet helper
│   ├── transform_daily_prices.py
│   ├── transform_fundamentals.py
│   ├── transform_overview.py
│   └── transform_corporate_actions.py
├── .github/
│   └── workflows/
│       ├── daily_prices.yml
│       └── weekly_refresh.yml
├── notebooks/                        # DuckDB analysis notebooks live here
├── .env.example                      # template — lists required env vars, no values
├── .gitignore                        # must include .env
├── requirements.txt
└── PROJECT_BRIEF.md
```
