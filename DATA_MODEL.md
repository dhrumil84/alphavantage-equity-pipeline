# Equity Data Pipeline — Data Model

## R2 Bucket Structure

Bucket name: `equity-data-lake`

```
equity-data-lake/
│
├── config/
│   └── ticker_universe.csv                    # master ticker list
│
├── bronze/                                    # raw API responses, immutable
│   ├── daily_prices/{symbol}/{pull_date}.json
│   ├── company_overview/{symbol}/{pull_date}.json
│   ├── income_statement/{symbol}/{pull_date}.json
│   ├── balance_sheet/{symbol}/{pull_date}.json
│   ├── cash_flow/{symbol}/{pull_date}.json
│   ├── earnings/{symbol}/{pull_date}.json
│   ├── dividends/{symbol}/{pull_date}.json
│   ├── splits/{symbol}/{pull_date}.json
│   ├── shares_outstanding/{symbol}/{pull_date}.json
│   ├── listing_status/{pull_date}.csv         # no ticker subfolder — universe-level
│   ├── earnings_calendar/{pull_date}.csv      # bronze only, no silver table
│   └── ipo_calendar/{pull_date}.json          # bronze only, no silver table
│
└── silver/                                    # cleaned, typed Parquet — query target
    ├── dim_company/
    │   └── dim_company.parquet
    ├── fact_daily_prices/
    │   ├── year=2005/fact_daily_prices.parquet
    │   ├── year=2006/fact_daily_prices.parquet
    │   │   ... (one partition per year)
    │   └── year=2026/fact_daily_prices.parquet
    ├── fact_income_statement/
    │   └── fact_income_statement.parquet
    ├── fact_balance_sheet/
    │   └── fact_balance_sheet.parquet
    ├── fact_cash_flow/
    │   └── fact_cash_flow.parquet
    ├── fact_earnings/
    │   └── fact_earnings.parquet
    ├── fact_dividends/
    │   └── fact_dividends.parquet
    └── fact_splits/
        └── fact_splits.parquet
```

---

## Bronze Layer Rules

Bronze files are written once and never modified. The only fields added at write time
(not present in the raw API response) are:

- `pull_date` (DATE): the date the API was called
- `symbol` (VARCHAR): the ticker symbol (some endpoints omit this from the response body)

Everything else is the raw API payload, preserved exactly as received including any
string "None" values, inconsistent casing, or unexpected fields.

---

## Silver Layer Rules

All silver tables enforce three rules without exception:

1. **Nulls are nulls.** Every `"None"` string value from the API is converted to SQL NULL.
   No string "None" ever appears in a typed silver column.

2. **Dates are dates.** Every date string (e.g. `"2026-03-06"`) is parsed to a proper
   DATE type. No date strings in silver.

3. **No derived metrics in silver.** Silver contains cleaned source data only. Ratios,
   returns, growth rates, and other computed fields belong in the gold layer (see below),
   in DuckDB queries, or in notebooks — not in silver.
   Exception: `free_cash_flow` is pre-computed in `fact_cash_flow` because Alpha Vantage
   does not return it reliably, and it is a direct arithmetic combination of two columns
   in the same row.

The gold layer (`gold/...`) is the designated home for derived metrics, pre-joined wide
tables, and pre-aggregated cohort statistics. Rules #1 and #2 (no string "None", proper
date types) still apply to gold. Rule #3 does not — gold exists specifically to hold
derived fields built from silver.

---

## Silver Table Schemas

### `dim_company`

One row per ticker. Rebuilt fully on each weekly refresh.
This is a Type 1 Slowly Changing Dimension (current state only — no history tracked).

| Column | Type | Source | Notes |
|---|---|---|---|
| symbol | VARCHAR | LISTING_STATUS / OVERVIEW | Primary key |
| name | VARCHAR | OVERVIEW | Company name |
| exchange | VARCHAR | LISTING_STATUS | NYSE, NASDAQ, etc. |
| asset_type | VARCHAR | LISTING_STATUS | Stock, ETF |
| cik | VARCHAR | OVERVIEW | SEC identifier |
| sector | VARCHAR | OVERVIEW | |
| industry | VARCHAR | OVERVIEW | |
| country | VARCHAR | OVERVIEW | |
| currency | VARCHAR | OVERVIEW | |
| fiscal_year_end | VARCHAR | OVERVIEW | e.g. "December" |
| listing_status | VARCHAR | LISTING_STATUS | 'active' or 'delisted' |
| ipo_date | DATE | LISTING_STATUS | |
| delisted_date | DATE | LISTING_STATUS | NULL if still active |
| market_cap | BIGINT | OVERVIEW | Snapshot at last pull |
| shares_outstanding | BIGINT | SHARES_OUTSTANDING | |
| last_updated | TIMESTAMP | pipeline | When this row was last written |

Deduplication key: `symbol`

---

### `fact_daily_prices`

One row per ticker per trading day. Partitioned by year for query performance.

| Column | Type | Source | Notes |
|---|---|---|---|
| symbol | VARCHAR | API | FK to dim_company |
| trade_date | DATE | API | |
| open | DECIMAL(18,4) | API | Raw as-traded price |
| high | DECIMAL(18,4) | API | |
| low | DECIMAL(18,4) | API | |
| close | DECIMAL(18,4) | API | Raw close |
| adjusted_close | DECIMAL(18,4) | API | ⭐ Use this for return calculations |
| volume | BIGINT | API | |
| dividend_amount | DECIMAL(18,6) | API | Non-zero on ex-dividend dates |
| split_coefficient | DECIMAL(10,6) | API | Non-1.0 on split dates |
| pull_date | DATE | pipeline | Ingestion date |

Deduplication key: `(symbol, trade_date)`

**Important:** Always use `adjusted_close` for return calculations and backtesting.
Raw `close` is kept for reference only. `adjusted_close` retroactively corrects for
splits and dividends so prices are comparable across time.

---

### `fact_income_statement`

One row per ticker per fiscal period per period type (annual or quarterly).

| Column | Type | Source | Notes |
|---|---|---|---|
| symbol | VARCHAR | API | |
| fiscal_date_ending | DATE | API | Period end date |
| period_type | VARCHAR | API | 'annual' or 'quarterly' |
| reported_currency | VARCHAR | API | |
| total_revenue | BIGINT | API | |
| gross_profit | BIGINT | API | |
| ebitda | BIGINT | API | |
| operating_income | BIGINT | API | |
| net_income | BIGINT | API | |
| eps_basic | DECIMAL(18,4) | API | |
| eps_diluted | DECIMAL(18,4) | API | |
| r_and_d | BIGINT | API | Research & Development |
| sga | BIGINT | API | Sales, General & Admin |
| interest_expense | BIGINT | API | |
| income_tax | BIGINT | API | |
| pull_date | DATE | pipeline | |

Deduplication key: `(symbol, fiscal_date_ending, period_type)`

---

### `fact_balance_sheet`

One row per ticker per fiscal period per period type.

| Column | Type | Source | Notes |
|---|---|---|---|
| symbol | VARCHAR | API | |
| fiscal_date_ending | DATE | API | |
| period_type | VARCHAR | API | 'annual' or 'quarterly' |
| reported_currency | VARCHAR | API | |
| total_assets | BIGINT | API | |
| total_liabilities | BIGINT | API | |
| total_equity | BIGINT | API | |
| cash_and_equivalents | BIGINT | API | |
| short_term_investments | BIGINT | API | |
| current_assets | BIGINT | API | |
| current_liabilities | BIGINT | API | |
| long_term_debt | BIGINT | API | |
| short_term_debt | BIGINT | API | |
| retained_earnings | BIGINT | API | |
| goodwill | BIGINT | API | |
| intangible_assets | BIGINT | API | |
| pull_date | DATE | pipeline | |

Deduplication key: `(symbol, fiscal_date_ending, period_type)`

---

### `fact_cash_flow`

One row per ticker per fiscal period per period type.

| Column | Type | Source | Notes |
|---|---|---|---|
| symbol | VARCHAR | API | |
| fiscal_date_ending | DATE | API | |
| period_type | VARCHAR | API | 'annual' or 'quarterly' |
| reported_currency | VARCHAR | API | |
| operating_cashflow | BIGINT | API | |
| capex | BIGINT | API | Capital expenditures |
| free_cash_flow | BIGINT | derived | operating_cashflow - abs(capex) |
| dividend_payout | BIGINT | API | |
| repurchase_of_stock | BIGINT | API | Buybacks |
| proceeds_from_debt | BIGINT | API | |
| repayment_of_debt | BIGINT | API | |
| investing_cashflow | BIGINT | API | |
| financing_cashflow | BIGINT | API | |
| change_in_cash | BIGINT | API | |
| pull_date | DATE | pipeline | |

Deduplication key: `(symbol, fiscal_date_ending, period_type)`

`free_cash_flow` is the only permitted derived field in the silver layer. It is
computed as `operating_cashflow - abs(capex)` during the bronze→silver transform.

---

### `fact_earnings`

One row per ticker per fiscal period per period type.

| Column | Type | Source | Notes |
|---|---|---|---|
| symbol | VARCHAR | API | |
| fiscal_date_ending | DATE | API | End of the fiscal period |
| period_type | VARCHAR | API | 'annual' or 'quarterly' |
| reported_eps | DECIMAL(18,4) | API | Actual reported EPS |
| estimated_eps | DECIMAL(18,4) | API | Consensus estimate |
| surprise | DECIMAL(18,4) | API | reported - estimated |
| surprise_pct | DECIMAL(10,4) | API | Percentage beat/miss |
| report_date | DATE | API | ⭐ Date earnings were publicly announced |
| pull_date | DATE | pipeline | |

Deduplication key: `(symbol, fiscal_date_ending, period_type)`

**Critical for backtesting — look-ahead bias:**
`fiscal_date_ending` is when the quarter ended (e.g. March 31).
`report_date` is when results were made public (e.g. April 28).
Always filter backtests using `report_date`, never `fiscal_date_ending`.
Using `fiscal_date_ending` implies you knew Q1 results on March 31 — you didn't.

---

### `fact_dividends`

One row per ticker per ex-dividend date.

| Column | Type | Notes |
|---|---|---|
| symbol | VARCHAR | |
| ex_date | DATE | The date that determines dividend eligibility |
| amount | DECIMAL(18,6) | Dividend per share |
| declared_date | DATE | |
| record_date | DATE | |
| payment_date | DATE | |
| pull_date | DATE | |

Deduplication key: `(symbol, ex_date)`

---

### `fact_splits`

One row per ticker per split event.

| Column | Type | Notes |
|---|---|---|
| symbol | VARCHAR | |
| effective_date | DATE | Date the split took effect |
| split_ratio | DECIMAL(10,4) | e.g. 4.0 for a 4-for-1 split |
| pull_date | DATE | |

Deduplication key: `(symbol, effective_date)`

---

## Gold Layer (Phase A)

Gold tables are purpose-built for analysis. They hold derived metrics,
pre-joined wide tables, and pre-aggregated cohort statistics. They are
fully rebuilt on each run — no upsert semantics. Silver remains the
source of truth; gold is a fast-access view over it.

R2 layout:

```
gold/
├── fact_fundamentals_wide/
│   └── fact_fundamentals_wide.parquet
├── fact_prices_enriched/
│   ├── year=2005/fact_prices_enriched.parquet
│   ├── ...
│   └── year=2026/fact_prices_enriched.parquet
└── dim_company_enriched/
    └── dim_company_enriched.parquet
```

### `fact_fundamentals_wide`

One row per `(symbol, fiscal_date_ending, period_type)`. Joins all four
silver fundamentals tables and adds derived columns.

| Group | Columns |
|---|---|
| Identifiers | symbol, fiscal_date_ending, period_type, reported_currency |
| Income stmt | total_revenue, gross_profit, ebitda, operating_income, net_income, eps_basic, eps_diluted, r_and_d, sga, interest_expense, income_tax |
| Balance sht | total_assets, total_liabilities, total_equity, cash_and_equivalents, short_term_investments, current_assets, current_liabilities, long_term_debt, short_term_debt, retained_earnings, goodwill, intangible_assets |
| Cash flow | operating_cashflow, capex, free_cash_flow, dividend_payout, repurchase_of_stock, proceeds_from_debt, repayment_of_debt, investing_cashflow, financing_cashflow, change_in_cash |
| Earnings | reported_eps, estimated_eps, surprise, surprise_pct, report_date |
| **Margins** | gross_margin, operating_margin, net_margin, fcf_margin, ebitda_margin |
| **Returns** | roe, roa, roic |
| **Leverage** | debt_to_equity, net_debt, interest_coverage |
| **Quality** | cash_conversion, accruals_ratio |
| **Growth** | revenue_growth_yoy, net_income_growth_yoy, eps_growth_yoy, fcf_growth_yoy, operating_cf_growth_yoy, revenue_growth_qoq |
| **TTM** (quarterly rows only) | total_revenue_ttm, net_income_ttm, operating_income_ttm, ebitda_ttm, free_cash_flow_ttm, operating_cashflow_ttm, eps_diluted_ttm, capex_ttm, dividend_payout_ttm |
| **EPS CAGR** | eps_cagr_5y, eps_cagr_3y, eps_cagr_as_of |
| Metadata | pull_date, gold_built_utc |

Notes:
- YoY uses 4-period lag for quarterly, 1-period lag for annual.
- TTM is `SUM(...) OVER (last 4 quarters)` — null on annual rows.
- EPS CAGR is computed on annual rows only, then propagated to all rows
  for the same symbol. Null when either endpoint EPS is non-positive
  (sign flips make CAGR meaningless).

Cadence: weekly (after `transform_fundamentals`).

### `fact_prices_enriched`

One row per `(symbol, trade_date)`. Partitioned by year.

| Group | Columns |
|---|---|
| Identifiers | symbol, trade_date |
| Raw (from silver) | open, high, low, close, adjusted_close, volume, dividend_amount, split_coefficient, pull_date |
| **Returns** | return_1d, return_5d, return_21d, return_63d, return_126d, return_252d |
| **Volatility** | volatility_30d, volatility_90d (annualized stdev of daily returns) |
| **Volume** | dollar_volume, volume_avg_20d, volume_ratio_20d |
| **52w range** | high_52w, low_52w, pct_off_52w_high, pct_off_52w_low, drawdown_from_52w_high |
| **MAs** | sma_20, sma_50, sma_200, ema_12, ema_26 |
| **MACD** | macd, macd_signal, macd_hist |
| **Bollinger** | bb_middle, bb_upper, bb_lower, bb_pct_b |
| **RSI** | rsi_14 (Wilder's smoothing) |
| **ATR** | atr_14 |
| **Relative strength vs SPY** | rel_strength_vs_spy_3m, rel_strength_vs_spy_6m, rel_strength_vs_spy_12m |
| Metadata | gold_built_utc |

Notes:
- All return / MA / Bollinger / RSI calculations use `adjusted_close`.
  ATR uses raw OHLC (it measures intra-day range, not corporate-action moves).
- Relative strength uses `(1 + r_ticker) / (1 + r_spy) - 1` so it's
  meaningful even when one leg is negative.
- Rebuilt fully each day. ~107 tickers × ~5000 days = ~500K rows.

Cadence: daily (after `transform_daily_prices`).

### `dim_company_enriched`

One row per symbol. Spine = `dim_company`. Joins latest TTM snapshot from
`fact_fundamentals_wide` and latest-price-derived returns from silver
prices.

| Group | Columns |
|---|---|
| All of silver `dim_company` | symbol, name, exchange, asset_type, cik, sector, industry, country, currency, fiscal_year_end, listing_status, ipo_date, delisted_date, market_cap, shares_outstanding, last_updated |
| Latest TTM | latest_quarter_end, total_revenue_ttm, net_income_ttm, ebitda_ttm, free_cash_flow_ttm, operating_cashflow_ttm, eps_diluted_ttm, dividend_payout_ttm |
| Latest margins | gross_margin, operating_margin, net_margin, fcf_margin |
| Latest returns on capital | roe, roa, roic |
| Latest leverage | debt_to_equity, net_debt |
| Latest growth | revenue_growth_yoy, eps_growth_yoy, fcf_growth_yoy |
| EPS CAGR | eps_cagr_5y, eps_cagr_3y |
| Latest price | as_of_date, latest_close, market_cap_latest |
| Trailing returns | price_return_1y, price_return_3y, price_return_5y |
| Metadata | gold_built_utc |

Notes:
- `market_cap_latest` = `latest_close × shares_outstanding`. Shares are
  point-in-time from `dim_company` (last weekly refresh).
- Trailing returns are price-only (not total return including dividends).
  True total return is a Phase B addition.

Cadence: weekly (after `build_fundamentals_wide`).

---

## DuckDB Query Patterns

### How to read silver Parquet files from R2

```python
import duckdb

con = duckdb.connect()

# Install and load the httpfs extension (needed for R2 access)
con.execute("INSTALL httpfs; LOAD httpfs;")

# Configure R2 credentials (read from environment variables)
con.execute(f"""
    SET s3_endpoint = '{r2_endpoint}';
    SET s3_access_key_id = '{r2_access_key}';
    SET s3_secret_access_key = '{r2_secret_key}';
    SET s3_region = 'auto';
""")

# Query any silver table directly
df = con.execute("""
    SELECT symbol, trade_date, adjusted_close
    FROM read_parquet('s3://equity-data-lake/silver/fact_daily_prices/**/*.parquet')
    WHERE symbol = 'AAPL'
    AND trade_date >= '2020-01-01'
    ORDER BY trade_date
""").df()
```

### Point-in-time universe filter (for backtesting)

```sql
-- Get all tickers that were active on a given analysis date
SELECT symbol
FROM read_parquet('s3://equity-data-lake/silver/dim_company/dim_company.parquet')
WHERE ipo_date <= '2022-01-01'
  AND (delisted_date IS NULL OR delisted_date > '2022-01-01')
  AND listing_status IN ('active', 'delisted')  -- include delisted to avoid survivorship bias
  AND asset_type = 'Stock'
```

### Simple return calculation across a basket

```sql
-- Calculate 1-year return for a basket of tickers
WITH prices AS (
    SELECT
        symbol,
        trade_date,
        adjusted_close,
        LAG(adjusted_close, 252) OVER (PARTITION BY symbol ORDER BY trade_date) AS price_1yr_ago
    FROM read_parquet('s3://equity-data-lake/silver/fact_daily_prices/**/*.parquet')
)
SELECT
    symbol,
    trade_date,
    ROUND((adjusted_close - price_1yr_ago) / price_1yr_ago * 100, 2) AS return_1yr_pct
FROM prices
WHERE price_1yr_ago IS NOT NULL
  AND trade_date = '2024-12-31'
ORDER BY return_1yr_pct DESC
```
