# Copilot instructions — Alpha Vantage Equity Pipeline

A personal ELT lakehouse: Alpha Vantage API → Cloudflare R2 (bronze → silver →
gold) → DuckDB notebooks, orchestrated by GitHub Actions cron. Read
`PROJECT_BRIEF.md`, `README.md`, `DATA_MODEL.md` and `docs/architecture/`
(especially the ADRs in `docs/architecture/decisions/`) before suggesting
structural changes.

These rules mirror `CLAUDE.md`, which guides Claude Code in this repo. Keep the
two files in sync: a rule changed in one should change in the other.

## Do not change without explicit instruction

- **Workflow schedules**: the `on: schedule: cron:` blocks in
  `.github/workflows/*.yml`.
- **Ticker lists in `config/`**: `ticker_universe.csv`, `index_universe.csv`,
  `vti_universe.csv`. Don't edit or regenerate them, and don't suggest running
  the scripts that rewrite them (`add_ticker.py`, `expand_universe.py`,
  `promote_to_universe.py`, `reconcile_universe.py`, `ensure_benchmarks.py`).
- **The tech stack** (below).

## Stack — fixed, do not substitute

Python 3.11+, `requests`, `boto3`, `pandas`, `pyarrow`, `duckdb`,
`python-dotenv`. Storage is Cloudflare R2 only; compute is GitHub Actions
runners only. Do not introduce a database (Postgres, SQLite), Spark/Databricks,
an ORM, Airflow/Dagster/Prefect, or new heavy dependencies. If a dependency is
truly needed, add it to `requirements.txt` (or `requirements-dev.txt` for
test/lint tooling) and say why in the PR.

## Layer rules (ADR 0002 — the most important rules in this repo)

- **Bronze is write-once.** `ingestion/` writes raw API responses verbatim.
  Per-symbol endpoints use `bronze/<endpoint>/<SYMBOL>/<pull_date>.json`;
  universe-wide ones use `bronze/<endpoint>/<pull_date>.<csv|json|xlsx>`
  (transcripts key by quarter). The README shows a Hive-style
  `endpoint=…/symbol=…` layout, but the code is authoritative; follow the
  existing pattern. Never overwrite, mutate, or delete bronze objects, not even
  to fix a parsing bug. Fix the parser and reprocess from existing bronze
  instead of re-pulling.
- **Silver is cleaned source data only.** `transform/` parses bronze into typed
  Parquet under `silver/<table>/`: strip `"None"`/empty strings to real `NULL`
  (no sentinel zeros), parse dates once to `DATE`, dedupe on the table's
  documented key (latest `pull_date` wins).
  **No derived metrics in silver.** The single exception is
  `fact_cash_flow.free_cash_flow`. Proposing a second exception requires a new
  ADR — flag it, don't add it silently.
- **Gold owns derived metrics.** Ratios, returns, growth, valuation, peer and
  sector comparisons go in `transform_gold/`. Gold tables are rebuilt in full
  each run via `transform_gold.utils.duckdb_silver.overwrite_parquet`, not
  upserted.
- Analysis reads silver/gold, never bronze.

## Shared utilities — always reuse, never reimplement

- HTTP to Alpha Vantage: `ingestion/utils/av_client.py` (`av_client.fetch`) —
  it owns retries, backoff, timeouts, and API call counting.
- Rate limiting: `ingestion/utils/rate_limiter.RateLimiter(calls_per_minute=75)`,
  calling `limiter.wait()` before every API call. All API traffic shares the
  75 calls/min budget.
- R2 access: `ingestion/utils/r2_client.py` (`key_exists`, `upload_json`,
  `download_bytes`, …). Don't create new boto3 clients in modules.
- Silver writes: `transform/utils/parquet_writer.upsert_parquet(df, key, dedup_keys)`.
- Gold reads/writes: `transform_gold/utils/duckdb_silver.py`
  (`duckdb_to_r2`, `silver_scan`, `gold_scan`, `overwrite_parquet`).
- Freshness skipping for slow-changing endpoints:
  `ingestion/utils/freshness.py` (`ENDPOINT_TTL_DAYS`, `build_fresh_symbol_set`).
- Per-run metrics: wrap `main()` in `observability.metrics.RunMetrics("<module_name>")`.

## Idempotency

Every script must be safe to re-run. Ingestion checks `r2_client.key_exists()`
(and freshness) before calling the API; silver upserts on its dedup key; gold
fully overwrites. Never write code that appends duplicates on a second run.

## Config, not code

The ticker universe comes from `config/ticker_universe.csv` (`symbol`, `name`,
`active`). Don't hardcode ticker lists in modules. Delisted tickers are never
deleted from bronze/silver (survivorship bias); point-in-time filters use
`ipo_date` and `delisted_date`.

## Secrets

Credentials come only from environment variables (`ALPHAVANTAGE_API_KEY`,
`R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME`),
loaded locally from a gitignored `.env` via `load_dotenv`, and from GitHub
Secrets in workflows. Never hardcode or log them. New variables go in
`.env.example` with no value.

## Code style

- One module per Alpha Vantage endpoint in `ingestion/` (`ingest_<name>.py`);
  silver builders are `transform/transform_<name>.py`; gold builders are
  `transform_gold/build_<table>.py`. Each is runnable as
  `python -m <package>.<module>` with a `main()` and an
  `if __name__ == "__main__":` guard.
- Gold builders start with a module docstring stating the output key, grain,
  sources, cadence/workflow ordering, and the `Run as:` command — follow that
  pattern for new builders.
- Use the `logging` module (module-level `logger`), not `print`. Per-symbol
  ingestion logs use the `[i/total] SYMBOL — outcome` format.
- In per-symbol loops, catch `av_client.AlphaVantageError` and log it so one bad
  symbol doesn't kill the whole run.
- `from __future__ import annotations` and type hints in new code. Comments
  explain *why* (rate budgets, API quirks, ordering constraints), not what.

## Data quality and workflows

- New silver/gold tables should get checks in `quality/checks.py` with an
  explicit severity: `critical` (fails the workflow), `warn`, or `info`.
- When adding a job, add it to `.github/workflows/daily_prices.yml` or
  `weekly_refresh.yml` in dependency order (ingest → silver → gold → DQ), with
  only the secrets that step needs, and keep `observability.storage_scan` last
  with `if: always()`. Note ordering constraints in a comment on the step.
- Update `README.md`, `DATA_MODEL.md` and `docs/architecture/` (data model,
  pipeline DAG) when adding endpoints or tables.

## Testing and lint

```bash
pip install -r requirements.txt -r requirements-dev.txt
python -m pytest
ruff check .
```

- Tests run fully offline: `tests/conftest.py` swaps `r2_client` for an on-disk
  fake (`fake_r2`) and blocks outbound HTTP, so no credentials are needed.
  Don't write tests that call the real Alpha Vantage API or R2.
- Fixtures in `tests/fixtures/` mirror Alpha Vantage response shapes (string
  values, `"None"` for missing). Gold tests point `duckdb_to_r2`/`silver_scan`
  at the fake bucket.
- **Every behavior change comes with added or updated tests.** A new transform
  gets a fixture plus a test asserting schema, null handling, dedup key, and
  idempotency.
- Ruff (`ruff.toml`) checks pyflakes plus `E4/E7/E9` only, not formatting.
  Imports placed after `load_dotenv()` carry `# noqa: E402`. `notebooks/` is
  excluded.
- The Tests and Lint workflows run on every PR.

## Working agreements

- Work on a branch and open a PR; never push to `main`.
- Keep changes scoped to the task; propose larger refactors instead of doing
  them.
- A PR description states what tests verified and what only a real pipeline run
  (with Alpha Vantage and R2 credentials) can verify.

## When reviewing pull requests

Prioritize, in order: bronze mutation or deletion; derived metrics leaking
into silver; changes to cron schedules or `config/` ticker lists that weren't
asked for; bypassing the shared rate limiter / AV client / R2 client;
non-idempotent writes or a missing/incorrect dedup key; hardcoded secrets or
tickers; `"None"` strings or sentinel values instead of NULL; workflow steps
in the wrong dependency order; behavior changes without tests. Style nits
come last.
