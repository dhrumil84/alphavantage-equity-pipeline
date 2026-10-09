# CLAUDE.md

Guidance for Claude Code sessions in this repo. Keep it short; the detailed
design lives in the docs linked below — read them rather than guessing.

## What this repo is

An Alpha Vantage → Cloudflare R2 lakehouse (bronze → silver → gold), orchestrated
by GitHub Actions cron jobs and queried locally with DuckDB.

- [`PROJECT_BRIEF.md`](PROJECT_BRIEF.md) — design principles and tech stack (final, do not substitute)
- [`README.md`](README.md) — layer contracts, module → table map, how to run steps
- [`DATA_MODEL.md`](DATA_MODEL.md) and [`docs/architecture/`](docs/architecture/) — table schemas, grains, DAGs, ADRs

## Do not change without explicit instruction

- **Workflow schedules** — the `on: schedule: cron:` blocks in `.github/workflows/*.yml`.
- **Ticker lists in `config/`** — `ticker_universe.csv`, `index_universe.csv`,
  `vti_universe.csv`. Don't edit, regenerate, or run the scripts that rewrite them
  (`add_ticker.py`, `expand_universe.py`, `promote_to_universe.py`,
  `reconcile_universe.py`, `ensure_benchmarks.py`).
- **The tech stack** — no new databases, compute platforms, or orchestration tools.

## Layer rules (enforce these in every change)

- **Bronze (`ingestion/`) is immutable.** Raw API responses are written verbatim
  and never rewritten or deleted. Per-symbol endpoints use
  `bronze/<endpoint>/<SYMBOL>/<pull_date>.json`; universe-wide ones use
  `bronze/<endpoint>/<pull_date>.<csv|json|xlsx>` (transcripts key by quarter).
  The README shows a Hive-style `endpoint=…/symbol=…` layout — the code is
  authoritative; follow the existing pattern.
- **All Alpha Vantage calls go through `ingestion/utils/av_client.py`** and the
  shared `RateLimiter` (75 calls/min). Never reimplement either per script.
- **All R2 I/O goes through `ingestion/utils/r2_client.py`.**
- **Silver (`transform/`)** = typed Parquet, `NULL` for missing values (no `"None"`
  strings or sentinel zeros), dates as `DATE`, **no derived metrics** (sole exception:
  `free_cash_flow`). Writes go through `transform/utils/parquet_writer.upsert_parquet`
  with the table's documented dedup key; latest `pull_date` wins.
- **Gold (`transform_gold/`)** reads silver via DuckDB (`transform_gold/utils/duckdb_silver.py`)
  and is rebuilt in full each run (`overwrite_parquet`), not upserted.
- **Every script is idempotent** — safe to re-run with no duplicate rows.
- **Delisted tickers are never deleted** from bronze or silver (survivorship bias).
- **Secrets come only from env vars** (`.env` locally, GitHub Secrets in Actions).

## Conventions

- Naming: `ingestion/ingest_<endpoint>.py` → `transform/transform_<endpoint>.py` →
  `transform_gold/build_<table>.py`. Shared helpers go in the layer's `utils/`.
- Run modules from the repo root as `python -m <package>.<module>`; transforms read
  `config/ticker_universe.csv` relative to the cwd.
- Python 3.11 in CI. Match the surrounding code's style; there is no linter configured.
- When adding a silver/gold table, update `README.md` and `DATA_MODEL.md`.

## Testing

```bash
pip install -r requirements.txt -r requirements-dev.txt
python -m pytest
```

- Tests run fully offline: `tests/conftest.py` replaces `r2_client` with an on-disk
  fake (`fake_r2`) and blocks outbound HTTP. They never need credentials.
- Fixtures in `tests/fixtures/` mirror Alpha Vantage response shapes (string values,
  `"None"` for missing). Gold tests point `duckdb_to_r2`/`silver_scan` at the fake bucket.
- **Add or update tests with every behavior change.** New transforms should get a
  fixture + a test asserting schema, null handling, dedup key, and idempotency.
- The Tests workflow (`.github/workflows/tests.yml`) runs on every PR.

## Working agreements

- Work on a branch and open a PR; never push to `main`.
- Keep changes scoped to what was asked; propose larger refactors instead of doing them.
- Cloud sessions have no Alpha Vantage or R2 credentials. In the PR description,
  state what was verified by tests and what can only be verified by a real pipeline run.
