# Alpha Vantage Equity Pipeline

A daily/weekly ELT pipeline that ingests US equity market data from the
[Alpha Vantage API](https://www.alphavantage.co/), lands it in a Cloudflare R2
lakehouse as bronze → silver → gold, and exposes the gold tables for analysis in
DuckDB notebooks. Orchestrated by GitHub Actions.

The working universe is ~1,100 US-listed tickers (large-cap core plus
micro/special situations), driven by [`config/ticker_universe.csv`](config/ticker_universe.csv).

## Architecture

```
Alpha Vantage API
        │  HTTP (rate-limited, 75 calls/min)
        ▼
   bronze/   raw JSON, immutable, partitioned by endpoint + symbol + pull_date
        ▼
   silver/   typed Parquet, deduped, no derived metrics
        ▼
   gold/     derived facts: ratios, returns, peer/sector relative
        ▼
   DuckDB notebooks (httpfs over R2)
```

All three layers live in a single Cloudflare R2 bucket under separate prefixes.

See [`docs/architecture/overview.md`](docs/architecture/overview.md) for the C4
diagrams and [`docs/architecture/pipeline-dag.md`](docs/architecture/pipeline-dag.md)
for the job DAGs.

## Layers

### Bronze — `ingestion/`

Each module pulls one Alpha Vantage endpoint and writes the raw JSON response
verbatim to `bronze/endpoint=<name>/symbol=<ticker>/pull_date=<YYYY-MM-DD>/`.
Bronze objects are never rewritten.

| Module | Endpoint |
|---|---|
| [`ingest_daily_prices.py`](ingestion/ingest_daily_prices.py) | TIME_SERIES_DAILY_ADJUSTED |
| [`ingest_listing_status.py`](ingestion/ingest_listing_status.py) | LISTING_STATUS (active + delisted) |
| [`ingest_overview.py`](ingestion/ingest_overview.py) | OVERVIEW |
| [`ingest_fundamentals.py`](ingestion/ingest_fundamentals.py) | INCOME_STATEMENT, BALANCE_SHEET, CASH_FLOW, EARNINGS |
| [`ingest_corporate_actions.py`](ingestion/ingest_corporate_actions.py) | dividends, splits |
| [`ingest_itot_holdings.py`](ingestion/ingest_itot_holdings.py) | iShares ITOT holdings (universe seed) |
| [`ingest_earnings_calendar.py`](ingestion/ingest_earnings_calendar.py) | EARNINGS_CALENDAR |

Cross-cutting concerns live in [`ingestion/utils/`](ingestion/utils/) — shared
HTTP client, token-bucket rate limiter, and R2 client. Ingestion is incremental:
each module checks whether the requested object already exists in bronze (or
silver, for price history) before calling the API.

### Silver — `transform/`

Reads bronze JSON and writes typed Parquet to `silver/<table_name>/`. Three rules:

- Nulls are `NULL` (no string `"None"`, no sentinel zeros).
- Dates are `DATE` (parsed once, at the edge).
- No derived metrics live in silver (sole exception: `free_cash_flow` =
  `operating_cash_flow - capital_expenditures`).

Tables produced:

| Builder | Silver table | Grain |
|---|---|---|
| [`transform_daily_prices.py`](transform/transform_daily_prices.py) | `fact_daily_prices` (year-partitioned) | symbol × trade_date |
| [`transform_overview.py`](transform/transform_overview.py) | `dim_company` | symbol |
| [`transform_fundamentals.py`](transform/transform_fundamentals.py) | `fact_income_statement`, `fact_balance_sheet`, `fact_cash_flow`, `fact_earnings` | symbol × fiscal_period |
| [`transform_corporate_actions.py`](transform/transform_corporate_actions.py) | `fact_dividends`, `fact_splits` | symbol × ex_date |

All silver writes upsert on a documented deduplication key, so re-running a
transform is idempotent.

### Gold — `transform_gold/`

Reads silver and writes derived analytical tables to `gold/<table_name>/`.

| Builder | Gold table | Contents |
|---|---|---|
| [`build_fundamentals_wide.py`](transform_gold/build_fundamentals_wide.py) | `fact_fundamentals_wide` | 79 cols: margins, returns on capital, leverage, YoY/QoQ growth, TTM rolling, 5y/3y EPS CAGR |
| [`build_prices_enriched.py`](transform_gold/build_prices_enriched.py) | `fact_prices_enriched` | returns 1d–252d, volatility, 52w range, SMA/EMA/MACD/Bollinger/RSI/ATR, relative strength vs SPY |
| [`build_dim_company_enriched.py`](transform_gold/build_dim_company_enriched.py) | `dim_company_enriched` | latest TTM metrics, market cap, trailing returns |
| [`build_valuation_daily.py`](transform_gold/build_valuation_daily.py) | `fact_valuation_daily` | daily P/E, P/B, EV/EBITDA, FCF yield, Elfenbein fair-PE heuristic |
| [`build_sector_aggregates.py`](transform_gold/build_sector_aggregates.py) | `fact_sector_aggregates` | sector/industry medians and percentiles |
| [`build_peer_relative.py`](transform_gold/build_peer_relative.py) | `fact_peer_relative` | per-ticker z-scores against peer group |

## Data quality — `quality/`

[`quality/checks.py`](quality/checks.py) defines severity-tagged assertions
(`critical` / `warn` / `info`) over silver and gold tables: null-rate thresholds,
data-freshness windows, symbol coverage, dedup-key uniqueness, and cross-table
referential checks.

[`quality/runner.py`](quality/runner.py) executes a named suite (`daily` or
`weekly`) against R2 via DuckDB, writes a JSON report to `reports/dq/`, and
exits non-zero if any `critical` check fails. The GitHub Actions workflows
invoke it directly, so a broken silver table fails the pipeline run.

## Observability — `observability/`

[`storage_scan.py`](observability/storage_scan.py) walks the R2 bucket and
writes a per-layer object-count and byte-size inventory to `reports/storage/`.
Runs on every workflow execution, including failures (`if: always()`), so we
always know what landed.

[`metrics.py`](observability/metrics.py) provides the per-run metrics helpers
used by ingestion and transform modules — API call counts, error counts,
duration — tagged with the GitHub Actions `run_id` and commit SHA and written
alongside the storage reports.

## Orchestration — GitHub Actions

Two scheduled workflows in [`.github/workflows/`](.github/workflows):

- **[`daily_prices.yml`](.github/workflows/daily_prices.yml)** — cron `0 23 * * 1-5`
  (6pm ET, weekdays). Ingest daily prices → transform to silver → build
  `fact_prices_enriched` → build `fact_valuation_daily` → run daily DQ suite →
  storage snapshot.
- **[`weekly_refresh.yml`](.github/workflows/weekly_refresh.yml)** — cron `0 6 * * 0`
  (Sunday 06:00 UTC). Refresh listing status, company overview, fundamentals,
  and corporate actions; rebuild all gold tables; run weekly DQ suite; storage
  snapshot.

Both workflows can also be triggered manually via `workflow_dispatch`.

## Local development

### Prerequisites

- Python 3.11+
- An Alpha Vantage API key
- Cloudflare R2 credentials and a bucket

### Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# fill in ALPHAVANTAGE_API_KEY, R2_ACCOUNT_ID, R2_ACCESS_KEY_ID,
#         R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME
```

### Run a single step

```bash
python -m ingestion.ingest_daily_prices --mode incremental
python -m transform.transform_daily_prices
python -m transform_gold.build_prices_enriched
python -m quality.runner --suite daily
python -m observability.storage_scan
```

### Explore the gold layer

Notebooks under [`notebooks/`](notebooks/) read gold tables directly from R2 via
DuckDB's `httpfs` extension — see [`validate_pipeline.ipynb`](notebooks/validate_pipeline.ipynb)
for the connection pattern.

### Price coverage validation notebooks

Use these notebooks when validating `silver/fact_daily_prices` coverage against
the expected start-date rule (`max(ipo_date, 1999-01-01)`):

- [`price_coverage_audit.ipynb`](notebooks/price_coverage_audit.ipynb)
     computes all-symbol coverage, late-start counts, and top offenders.
- [`price_coverage_diagnostics.ipynb`](notebooks/price_coverage_diagnostics.ipynb)
     drills into selected symbols with silver evidence + bronze key metadata and
     includes a remediation checklist.

Recommended flow: run audit first, then diagnostics for symbols flagged as
late-start or missing prices.

## Repository layout

```
config/           ticker universe + DQ thresholds
ingestion/        bronze writers (one module per AV endpoint)
transform/        bronze → silver builders
transform_gold/   silver → gold builders
quality/          DQ checks + runner
observability/    storage scan + metrics
notebooks/        DuckDB exploration & validation
docs/             architecture diagrams, ADRs, data model
.github/workflows daily + weekly cron pipelines
```
