# Equity Data Pipeline — Implementation Plan

## How to Use This Document

This plan is structured for use with an agentic IDE (Google Antigravity).
Each task is a discrete, reviewable unit of work. The recommended workflow:

1. Open Agent Manager in Antigravity
2. Reference PROJECT_BRIEF.md and DATA_MODEL.md as context
3. Assign **one task at a time**
4. Review the output before approving and moving to the next task
5. Commit after each task passes review

Do not ask the agent to complete multiple tasks in one session.
Each task builds on the previous one — skipping ahead risks compounding errors.

---

## Phase 0 — Repository Setup (Do This Manually Before Any Agent Work)

These steps require human decisions and credentials. Do them yourself before
opening Antigravity.

- [ ] Create GitHub repository: `equity-pipeline`
- [ ] Connect repository to Antigravity
- [ ] Sign up for Cloudflare R2, create bucket: `equity-data-lake`
- [ ] Generate R2 API token with read/write permissions
- [ ] Add GitHub Secrets to the repository:
  - `ALPHAVANTAGE_API_KEY`
  - `R2_ACCOUNT_ID`
  - `R2_ACCESS_KEY_ID`
  - `R2_SECRET_ACCESS_KEY`
  - `R2_BUCKET_NAME` = `equity-data-lake`
- [ ] Add PROJECT_BRIEF.md, DATA_MODEL.md, and IMPLEMENTATION_PLAN.md to the repo root

---

## Phase 1 — Foundation (Shared Utilities)

These files are used by every script in the pipeline. Build them first.
Nothing downstream works correctly until these are solid.

### Task 1.1 — Project scaffolding and config

**Prompt for agent:**
> "Create the repository folder structure defined in PROJECT_BRIEF.md under the Repository
> Structure section. Create empty `__init__.py` files where needed. Create a
> `requirements.txt` with these packages: `requests`, `boto3`, `pandas`, `pyarrow`,
> `python-dotenv`, `duckdb`. Create a `.env.example` file listing all required environment
> variables (R2 and Alpha Vantage) with placeholder values. Create a `.gitignore` that
> includes `.env`, `__pycache__`, `.DS_Store`, and `*.pyc`. Create
> `config/ticker_universe.csv` with columns `symbol,name,active` and seed it with these
> 5 tickers: AAPL, MSFT, GOOGL, JPM, XOM."

**Review checklist:**
- [ ] Folder structure matches PROJECT_BRIEF.md exactly
- [ ] `.env` is in `.gitignore`
- [ ] All 5 seed tickers present in `ticker_universe.csv`
- [ ] No actual secrets in any file

---

### Task 1.2 — R2 client utility

**Prompt for agent:**
> "Create `ingestion/utils/r2_client.py`. This module provides a thin wrapper around
> boto3's S3 client configured for Cloudflare R2. It must read credentials from
> environment variables: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
> R2_BUCKET_NAME. The R2 endpoint URL format is:
> `https://{account_id}.r2.cloudflarestorage.com`.
> Provide these functions:
> - `upload_json(data: dict, key: str)` — serialise dict to JSON and upload to R2
> - `upload_bytes(data: bytes, key: str)` — upload raw bytes
> - `download_json(key: str) -> dict` — download and deserialise JSON from R2
> - `key_exists(key: str) -> bool` — check if an object exists without downloading it
> - `list_keys(prefix: str) -> list[str]` — list all object keys under a prefix
> Include docstrings on all public functions. Include a simple test at the bottom
> under `if __name__ == '__main__'` that uploads and retrieves a small test JSON."

**Review checklist:**
- [ ] No credentials hardcoded
- [ ] Endpoint URL constructed correctly using account ID
- [ ] `key_exists` uses HEAD request, not a full download
- [ ] Error handling for missing keys (should return False, not raise)

---

### Task 1.3 — Alpha Vantage HTTP client

**Prompt for agent:**
> "Create `ingestion/utils/av_client.py`. This module handles all HTTP communication
> with the Alpha Vantage API. It must:
> - Read the API key from environment variable `ALPHAVANTAGE_API_KEY`
> - Base URL: `https://www.alphavantage.co/query`
> - Provide a single function `fetch(params: dict) -> dict` that adds the apikey to
>   params, makes the GET request, checks for Alpha Vantage error responses
>   (their errors return HTTP 200 with a JSON body containing 'Error Message' or
>   'Information' keys — check for both), and returns the parsed JSON on success.
> - Raise a descriptive exception on API errors or HTTP errors.
> - Log the function name being called (from params) and the symbol if present."

**Review checklist:**
- [ ] API key not hardcoded
- [ ] Both Alpha Vantage error patterns detected ('Error Message' AND 'Information')
- [ ] HTTP errors (non-200) also handled
- [ ] Logging present

---

### Task 1.4 — Rate limiter utility

**Prompt for agent:**
> "Create `ingestion/utils/rate_limiter.py`. This implements a token bucket rate
> limiter that enforces a maximum of 75 API calls per 60 seconds. It must be usable
> as a context manager or a simple callable. Usage pattern:
>
>     limiter = RateLimiter(calls_per_minute=75)
>     for symbol in symbols:
>         limiter.wait()   # blocks if needed to stay under the limit
>         result = av_client.fetch(...)
>
> The implementation should track call timestamps in a deque and block using
> `time.sleep()` when the rate would be exceeded. Include a docstring explaining
> the token bucket approach."

**Review checklist:**
- [ ] Does not allow more than 75 calls in any rolling 60-second window
- [ ] `wait()` method blocks, does not raise
- [ ] Thread-safe (uses a lock)

---

## Phase 2 — Bronze Ingestion Scripts

Each script pulls from one API endpoint family and writes raw JSON to R2.
All scripts share a common pattern:
1. Read `config/ticker_universe.csv` (where relevant)
2. For each active ticker, check if today's bronze file already exists in R2
3. If it exists, skip (idempotency)
4. If not, call the API (respecting rate limiter) and write the raw JSON to bronze

---

### Task 2.1 — Listing status ingestion

**Prompt for agent:**
> "Create `ingestion/ingest_listing_status.py`. This script calls the Alpha Vantage
> LISTING_STATUS endpoint, which returns a CSV of all listed and delisted US tickers.
> It is a single API call (not a per-ticker loop).
>
> API call: `function=LISTING_STATUS&state=active` and separately `state=delisted`
>
> Bronze path for the output:
> `bronze/listing_status/{pull_date}.csv` (e.g. `bronze/listing_status/2026-03-06.csv`)
>
> The script should:
> - Check if today's file already exists in R2 before calling the API
> - Write the raw CSV bytes directly to R2 (do not parse)
> - Log how many bytes were written
> - Be runnable directly: `python -m ingestion.ingest_listing_status`"

**Review checklist:**
- [ ] Both `active` and `delisted` states fetched and stored separately
- [ ] Idempotency check present (skips if today's file exists)
- [ ] Raw CSV written, not parsed
- [ ] Uses `r2_client` and `av_client` utilities, not direct boto3/requests

---

### Task 2.2 — Daily prices ingestion

**Prompt for agent:**
> "Create `ingestion/ingest_daily_prices.py`. This script ingests daily adjusted price
> data for all active tickers in `config/ticker_universe.csv`.
>
> API function: `TIME_SERIES_DAILY_ADJUSTED`, with `outputsize=full` for the initial
> historical load and `outputsize=compact` (last 100 days) for incremental runs.
>
> The script should accept a `--mode` argument: `full` or `incremental`.
> In `full` mode: always fetch with outputsize=full.
> In `incremental` mode: skip any ticker where today's bronze file already exists.
>
> Bronze path: `bronze/daily_prices/{symbol}/{pull_date}.json`
>
> Use the shared rate limiter between each API call. Log progress as:
> `[{i}/{total}] {symbol} — written / skipped`"

**Review checklist:**
- [ ] `--mode` argument works for both full and incremental
- [ ] Rate limiter applied between every API call
- [ ] Bronze path matches DATA_MODEL.md exactly
- [ ] Skips already-existing files in incremental mode

---

### Task 2.3 — Fundamentals ingestion

**Prompt for agent:**
> "Create `ingestion/ingest_fundamentals.py`. This script ingests four fundamental
> data endpoints for each active ticker: INCOME_STATEMENT, BALANCE_SHEET, CASH_FLOW,
> and EARNINGS.
>
> For each ticker, make four API calls (one per endpoint) and write four separate
> bronze files:
> - `bronze/income_statement/{symbol}/{pull_date}.json`
> - `bronze/balance_sheet/{symbol}/{pull_date}.json`
> - `bronze/cash_flow/{symbol}/{pull_date}.json`
> - `bronze/earnings/{symbol}/{pull_date}.json`
>
> Apply idempotency: if all four files for a ticker already exist for today, skip
> that ticker entirely. If some exist and some do not, only fetch the missing ones.
>
> Use the shared rate limiter. These four calls per ticker count against the same
> 75/min limit as all other calls."

**Review checklist:**
- [ ] Four endpoints per ticker
- [ ] Partial idempotency (skip only the files that already exist)
- [ ] Rate limiter applied to every individual API call
- [ ] All four bronze paths match DATA_MODEL.md exactly

---

### Task 2.4 — Company overview ingestion

**Prompt for agent:**
> "Create `ingestion/ingest_overview.py`. This script ingests the OVERVIEW endpoint
> for all active tickers.
>
> Bronze path: `bronze/company_overview/{symbol}/{pull_date}.json`
>
> This endpoint can return an empty dict `{}` for some tickers (ETFs, foreign-listed,
> thinly-covered stocks). Detect this case and log a warning but do not raise an error.
> Still write the empty response to bronze so we have a record that the ticker was
> attempted."

**Review checklist:**
- [ ] Empty dict response handled gracefully (warn, don't crash)
- [ ] Empty responses still written to bronze
- [ ] Rate limiter applied

---

### Task 2.5 — Corporate actions ingestion

**Prompt for agent:**
> "Create `ingestion/ingest_corporate_actions.py`. This script ingests three endpoints
> for each active ticker: DIVIDENDS, SPLITS, and SHARES_OUTSTANDING.
>
> Bronze paths:
> - `bronze/dividends/{symbol}/{pull_date}.json`
> - `bronze/splits/{symbol}/{pull_date}.json`
> - `bronze/shares_outstanding/{symbol}/{pull_date}.json`
>
> Same idempotency and rate limiting pattern as the other ingestion scripts."

---

### Task 2.6 — Earnings calendar ingestion

**Prompt for agent:**
> "Create `ingestion/ingest_earnings_calendar.py`. This script calls the EARNINGS_CALENDAR endpoint
> (a single API call, not per-ticker). It returns a CSV.
> Write the raw CSV bytes directly to bronze, same pattern as listing status.
> Apply idempotency: skip if today's file already exists.
>
> Bronze path: `bronze/earnings_calendar/{pull_date}.csv`"

---

## Phase 3 — Bronze → Silver Transformers

Each transformer reads the latest available bronze file for each ticker,
parses and cleans the data, and writes (or appends/upserts) to the appropriate
silver Parquet table. Refer to DATA_MODEL.md for exact column names and types.

### Task 3.1 — Shared Parquet writer utility

**Prompt for agent:**
> "Create `transform/utils/parquet_writer.py`. This module handles writing Parquet
> files to the silver layer of R2. It must provide:
>
> `upsert_parquet(new_df: pd.DataFrame, s3_key: str, dedup_keys: list[str])`
>
> This function should:
> 1. Download the existing Parquet file from R2 if it exists
> 2. Concatenate the existing data with `new_df`
> 3. Deduplicate on `dedup_keys`, keeping the row with the latest `pull_date`
> 4. Write the result back to R2 as Parquet using pyarrow
>
> Use pyarrow for Parquet serialisation (not pandas `.to_parquet()` directly).
> Ensure all date columns are written as pyarrow date32 type, not strings."

**Review checklist:**
- [ ] Deduplication keeps latest `pull_date` on key conflicts
- [ ] Handles the case where no existing file exists in R2
- [ ] Uses pyarrow, not just pandas
- [ ] Date types are date32, not strings

---

### Task 3.2 — Transform daily prices

**Prompt for agent:**
> "Create `transform/transform_daily_prices.py`. This transformer reads bronze daily
> price JSON files and writes to `silver/fact_daily_prices/year={year}/` partitioned
> Parquet files.
>
> For each active ticker, find the most recent bronze file under
> `bronze/daily_prices/{symbol}/`. Parse the Alpha Vantage TIME_SERIES_DAILY_ADJUSTED
> response format. Apply the silver rules from DATA_MODEL.md:
> - Convert all 'None' strings to NULL
> - Parse all date strings to DATE type
> - Map API field names to the silver column names in DATA_MODEL.md
> - Write to the correct year partition based on trade_date
>
> Deduplication key: `(symbol, trade_date)`
>
> After running, log how many new rows were written vs. already existed."

---

### Task 3.3 — Transform fundamentals

**Prompt for agent:**
> "Create `transform/transform_fundamentals.py`. This transformer processes the four
> fundamental bronze endpoints (income_statement, balance_sheet, cash_flow, earnings)
> and writes to their respective silver Parquet tables as defined in DATA_MODEL.md.
>
> Key requirements:
> - Both annual and quarterly reports are in the same bronze JSON. Write both to the
>   same silver table with `period_type` = 'annual' or 'quarterly'.
> - Deduplication key for all three financial statements:
>   `(symbol, fiscal_date_ending, period_type)`
> - For `fact_cash_flow`, compute `free_cash_flow = operating_cashflow - abs(capex)`.
>   Treat NULL capex as zero for this calculation only.
> - For `fact_earnings`, preserve both `fiscal_date_ending` AND `report_date` as
>   separate DATE columns. Do not conflate them.
> - Apply all three silver rules (nulls, dates, no other derived metrics)."

---

### Task 3.4 — Transform dim_company

**Prompt for agent:**
> "Create `transform/transform_overview.py`. This transformer builds the `dim_company`
> silver table by joining two data sources:
> 1. The most recent `bronze/listing_status/{pull_date}.csv` — provides symbol,
>    exchange, asset_type, ipo_date, delisted_date, listing_status
> 2. The most recent `bronze/company_overview/{symbol}/{pull_date}.json` for each
>    ticker — provides name, sector, industry, cik, currency, fiscal_year_end,
>    market_cap
>
> The listing_status CSV is the authoritative source for which tickers exist.
> Overview data enriches it. If no overview file exists for a ticker, write the
> row with NULLs for overview-sourced columns — do not skip the ticker.
>
> This table is fully rebuilt (not upserted) on every run.
> Write to: `silver/dim_company/dim_company.parquet`"

---

### Task 3.5 — Transform corporate actions

**Prompt for agent:**
> "Create `transform/transform_corporate_actions.py`. This transformer processes
> bronze dividends, splits, and shares_outstanding data into their silver tables
> as defined in DATA_MODEL.md.
>
> Deduplication keys:
> - `fact_dividends`: `(symbol, ex_date)`
> - `fact_splits`: `(symbol, effective_date)`
>
> `shares_outstanding` data rolls up into the `dim_company` table
> (the `shares_outstanding` column), not a separate fact table. Update that column
> when this transformer runs."

---

## Phase 4 — GitHub Actions Workflows

### Task 4.1 — Daily prices workflow

**Prompt for agent:**
> "Create `.github/workflows/daily_prices.yml`. This workflow:
> - Runs on a cron schedule: weekdays at 23:00 UTC (6pm ET + buffer)
> - Can also be triggered manually via workflow_dispatch
> - Checks out the repo
> - Sets up Python 3.11
> - Installs requirements.txt
> - Runs `python -m ingestion.ingest_daily_prices --mode incremental`
> - Then runs `python -m transform.transform_daily_prices`
> - Injects all GitHub Secrets as environment variables:
>   ALPHAVANTAGE_API_KEY, R2_ACCOUNT_ID, R2_ACCESS_KEY_ID,
>   R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME"

---

### Task 4.2 — Weekly refresh workflow

**Prompt for agent:**
> "Create `.github/workflows/weekly_refresh.yml`. This workflow:
> - Runs on a cron schedule: Sundays at 06:00 UTC
> - Can also be triggered manually via workflow_dispatch
> - Runs these scripts in order (each must complete before the next starts):
>   1. `python -m ingestion.ingest_listing_status`
>   2. `python -m ingestion.ingest_overview`
>   3. `python -m ingestion.ingest_fundamentals`
>   4. `python -m ingestion.ingest_corporate_actions`
>   5. `python -m transform.transform_overview`
>   6. `python -m transform.transform_fundamentals`
>   7. `python -m transform.transform_corporate_actions`
> - Inject all GitHub Secrets as environment variables."

---

## Phase 5 — Validation Notebook

### Task 5.1 — DuckDB validation notebook

**Prompt for agent:**
> "Create `notebooks/validate_pipeline.ipynb`. This Jupyter notebook validates
> that the pipeline ran correctly. It should:
> 1. Connect to R2 using DuckDB's httpfs extension (credentials from .env)
> 2. Run a query against each silver table and print row counts
> 3. Check that `dim_company` has the expected tickers
> 4. Run the simple return calculation example from DATA_MODEL.md
> 5. Check for any NULL values in key columns (trade_date, symbol, adjusted_close)
> 6. Print a summary pass/fail for each check
>
> All R2 credentials should be read from environment variables, never hardcoded."

---

## Completion Checklist

After all phases are done, verify end-to-end:

- [ ] Run Phase 0 setup manually
- [ ] Phase 1: all four utilities pass manual review
- [ ] Phase 2: run all ingest scripts locally against 5 seed tickers, confirm bronze files appear in R2
- [ ] Phase 3: run all transform scripts, confirm silver Parquet files appear in R2
- [ ] Phase 4: manually trigger both GitHub Actions workflows, confirm they succeed
- [ ] Phase 5: run validation notebook, all checks pass
- [ ] Expand `ticker_universe.csv` to S&P 500 and re-run Phase 2/3

---

## Extending the Pipeline Later

When you are ready to add new endpoints (Options, Alpha Intelligence, etc.):

1. Add a new entry to this document as a new Phase
2. Add a new bronze folder to the R2 structure in DATA_MODEL.md
3. Create a new ingestion script (same pattern as Phase 2)
4. Create a new transformer and silver table (same pattern as Phase 3)
5. Add the new script to the appropriate GitHub Actions workflow

No existing files need to be modified to add a new data source.
