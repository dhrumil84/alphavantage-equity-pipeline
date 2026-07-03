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
| cost_of_revenue | BIGINT | API | |
| gross_profit | BIGINT | API | |
| operating_expenses | BIGINT | API | |
| operating_income | BIGINT | API | |
| ebit | BIGINT | API | Earnings before interest & tax |
| ebitda | BIGINT | API | |
| depreciation_amortization | BIGINT | API | |
| income_before_tax | BIGINT | API | |
| income_tax | BIGINT | API | |
| net_income | BIGINT | API | |
| net_income_continuing_ops | BIGINT | API | Excludes discontinued operations |
| r_and_d | BIGINT | API | Research & Development |
| sga | BIGINT | API | Sales, General & Admin |
| interest_expense | BIGINT | API | |
| pull_date | DATE | pipeline | |

Deduplication key: `(symbol, fiscal_date_ending, period_type)`

**EPS is not in this table.** Alpha Vantage's `INCOME_STATEMENT` endpoint does
not return EPS fields. Use `fact_earnings.reported_eps` instead.

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

**Note on `annualEarnings` filtering:**
Alpha Vantage's `EARNINGS` endpoint's `annualEarnings` array contains a
spurious rolling-TTM entry at the most recent quarter end, alongside the
true fiscal-year-end annual entries. The silver transform filters these
out by detecting the dominant fiscal-year-end MM-DD pattern across all
entries and keeping only matching rows. Silver should contain only true
fiscal-year-end annual rows.

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

## Silver Layer — Extensions (News, Transcripts, Insider, Institutional, ETF, Indices)

These tables ingest the Alpha Intelligence + ETF + Index endpoints added on top of
the original core. Same silver rules apply: nulls are nulls, dates are dates, no
derived metrics (derived columns live in gold).

### `fact_news_articles`

One row per unique news article. Bronze sourced from both per-topic and per-ticker
NEWS_SENTIMENT calls; dedup on URL.

| Column | Type | Notes |
|---|---|---|
| url | VARCHAR | Primary key |
| title | VARCHAR | |
| time_published | TIMESTAMP | Parsed from `YYYYMMDDTHHMM[SS]` |
| authors | VARCHAR | JSON-encoded list |
| summary | VARCHAR | |
| source | VARCHAR | |
| source_domain | VARCHAR | |
| category_within_source | VARCHAR | |
| overall_sentiment_score | DOUBLE | -1.0 to +1.0 |
| overall_sentiment_label | VARCHAR | Bearish / Somewhat-Bearish / Neutral / Somewhat-Bullish / Bullish |
| topics | VARCHAR | JSON-encoded list of `{topic, relevance_score}` |
| banner_image | VARCHAR | |
| pull_date | DATE | |

Deduplication key: `url`

### `fact_news_ticker_sentiment`

Exploded one row per `(article, ticker)`. An article that mentions 4 tickers
produces 4 rows here.

| Column | Type | Notes |
|---|---|---|
| url | VARCHAR | FK to `fact_news_articles.url` |
| ticker | VARCHAR | |
| relevance_score | DOUBLE | 0.0 to 1.0 |
| ticker_sentiment_score | DOUBLE | -1.0 to +1.0 |
| ticker_sentiment_label | VARCHAR | Bearish / Somewhat-Bearish / Neutral / Somewhat-Bullish / Bullish |
| pull_date | DATE | |

Deduplication key: `(url, ticker)`

---

### `dim_earnings_calls`

One row per `(symbol, quarter)`. `call_date` enriched from `fact_earnings.report_date`.

| Column | Type | Notes |
|---|---|---|
| symbol | VARCHAR | |
| quarter | VARCHAR | `YYYYQN`, e.g. `2024Q1` |
| call_date | DATE | Nullable; sourced from `fact_earnings.report_date` |
| participant_count | INT | Distinct speakers in the transcript |
| pull_date | DATE | |

Deduplication key: `(symbol, quarter)`

### `fact_transcript_turns`

One row per speaker turn in a transcript. AV provides per-turn sentiment.

| Column | Type | Notes |
|---|---|---|
| symbol | VARCHAR | |
| quarter | VARCHAR | |
| turn_idx | INT | 0-based position in the transcript array |
| speaker | VARCHAR | |
| title | VARCHAR | Role/title (e.g. "CEO", "Analyst — Morgan Stanley") |
| content | VARCHAR | Full text of the turn |
| sentiment | DOUBLE | Per-turn sentiment provided by AV |
| pull_date | DATE | |

Deduplication key: `(symbol, quarter, turn_idx)`

---

### `fact_insider_transactions`

One row per insider Form 4 transaction. No single PK — Form 4 can have multiple
identical-looking lines for the same exec on the same day; composite key.

| Column | Type | Notes |
|---|---|---|
| symbol | VARCHAR | |
| transaction_date | DATE | |
| executive | VARCHAR | |
| executive_title | VARCHAR | |
| security_type | VARCHAR | e.g. "Common Stock" |
| acquisition_or_disposal | VARCHAR | `A` or `D` |
| shares | DOUBLE | |
| share_price | DOUBLE | Can be 0.0 (e.g. RSU vests, gifts) |
| pull_date | DATE | |

Deduplication key: `(symbol, transaction_date, executive, shares, share_price, acquisition_or_disposal)`

---

### `dim_institutional_summary`

Snapshot of institutional ownership aggregates per pull.

| Column | Type | Notes |
|---|---|---|
| symbol | VARCHAR | |
| total_institutional_holders | INT | |
| total_institutional_shares | DOUBLE | |
| holders_with_increased_holdings | INT | |
| shares_with_increased_holdings | DOUBLE | |
| holders_with_decreased_holdings | INT | |
| shares_with_decreased_holdings | DOUBLE | |
| holders_with_unchanged_holdings | INT | |
| shares_with_unchanged_holdings | DOUBLE | |
| total_institutional_ownership_percentage | DOUBLE | |
| pull_date | DATE | |

Deduplication key: `(symbol, pull_date)`

### `fact_institutional_holdings`

Per-holder rows from the `holdings[]` array. Schema is defensive — populated from
whichever fields are present in the API payload.

| Column | Type | Notes |
|---|---|---|
| symbol | VARCHAR | |
| holder_name | VARCHAR | |
| shares_held | DOUBLE | |
| shares_change | DOUBLE | |
| pct_of_portfolio | DOUBLE | |
| market_value | DOUBLE | |
| report_date | DATE | Quarter end of the 13F filing |
| pull_date | DATE | |

Deduplication key: `(symbol, holder_name, pull_date)`

---

### `dim_etf_profile`

ETF-level metadata (one row per ETF, latest snapshot).

| Column | Type | Notes |
|---|---|---|
| symbol | VARCHAR | |
| net_assets | DOUBLE | AUM |
| net_expense_ratio | DOUBLE | |
| portfolio_turnover | DOUBLE | |
| dividend_yield | DOUBLE | |
| inception_date | DATE | |
| leveraged | VARCHAR | `YES` / `NO` |
| pull_date | DATE | |

Deduplication key: `symbol`

### `fact_etf_holdings`

Constituent rows for each ETF snapshot.

| Column | Type | Notes |
|---|---|---|
| etf_symbol | VARCHAR | |
| holding_symbol | VARCHAR | |
| description | VARCHAR | Long name of the constituent |
| weight | DOUBLE | Portfolio weight (0..1) |
| as_of_date | DATE | = pull_date (no explicit holdings-date in payload) |
| pull_date | DATE | |

Deduplication key: `(etf_symbol, holding_symbol, as_of_date)`

### `fact_etf_sector_allocation`

Sector allocation rows from the `sectors[]` array.

| Column | Type | Notes |
|---|---|---|
| etf_symbol | VARCHAR | |
| sector | VARCHAR | e.g. `INFORMATION TECHNOLOGY` |
| weight | DOUBLE | |
| as_of_date | DATE | |
| pull_date | DATE | |

Deduplication key: `(etf_symbol, sector, as_of_date)`

---

### `fact_index_prices`

Daily OHLC for major market indices (DJI, SPX, COMP, NDX, VIX, RUT, OEX, ...).
Year-partitioned, mirrors `fact_daily_prices` but with **no volume** column —
indices don't report aggregate volume through INDEX_DATA.

| Column | Type | Notes |
|---|---|---|
| index_symbol | VARCHAR | Distinct from `symbol` to avoid confusion with equities |
| trade_date | DATE | |
| open | DOUBLE | |
| high | DOUBLE | |
| low | DOUBLE | |
| close | DOUBLE | |
| pull_date | DATE | |

Deduplication key: `(index_symbol, trade_date)`
Partitioning: `year=YYYY/`

---

## Gold Layer (Phase A + B)

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
├── dim_company_enriched/
│   └── dim_company_enriched.parquet
├── fact_valuation_daily/                       # Phase B
│   ├── year=2000/fact_valuation_daily.parquet
│   ├── ...
│   └── year=2026/fact_valuation_daily.parquet
├── fact_sector_aggregates/                     # Phase B
│   └── fact_sector_aggregates.parquet
└── fact_peer_relative/                         # Phase B
    └── fact_peer_relative.parquet
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
| **TTM** (quarterly rows only) | total_revenue_ttm, net_income_ttm, operating_income_ttm, ebitda_ttm, free_cash_flow_ttm, operating_cashflow_ttm, reported_eps_ttm, capex_ttm, dividend_payout_ttm |
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
| Latest TTM | latest_quarter_end, total_revenue_ttm, net_income_ttm, ebitda_ttm, free_cash_flow_ttm, operating_cashflow_ttm, reported_eps_ttm, dividend_payout_ttm |
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

### `fact_valuation_daily` (Phase B)

One row per `(symbol, trade_date)`. Daily-cadence valuation snapshot.
Year-partitioned, full rebuild per run.

| Group | Columns |
|---|---|
| Identifiers | symbol, trade_date |
| Spot | close, adjusted_close, sector, industry, shares_outstanding |
| As-of fundamentals | fundamentals_as_of (the source `fiscal_date_ending`), fundamentals_effective_date (when those numbers became public — `COALESCE(report_date, fiscal_date_ending + 60d)`), reported_eps_ttm |
| **Market cap / EV** | market_cap (= close × shares_outstanding), enterprise_value (= market_cap + long_term_debt + short_term_debt − cash_and_equivalents − short_term_investments) |
| **Ratios** | pe_ttm, ps_ttm, pb, ev_ebitda_ttm, fcf_yield_ttm, dividend_yield_ttm |
| **Growth** | eps_cagr_5y, eps_cagr_3y (propagated from fact_fundamentals_wide) |
| **Elfenbein fair-PE heuristic** | elfenbein_fair_pe (= eps_cagr_5y_pct/2 + 8), elfenbein_fair_pe_3y, elfenbein_fair_price (= fair_pe × reported_eps_ttm), elfenbein_fair_price_3y, elfenbein_upside_pct, elfenbein_margin_of_safety_met (BOOLEAN, true when close ≤ 0.7 × fair_price) |
| Metadata | gold_built_utc |

Notes:
- **As-of join** to fundamentals via DuckDB `ASOF JOIN` keyed on
  `trade_date >= effective_date`. Prevents look-ahead bias: a daily row
  dated 2024-03-15 only sees fundamentals publicly reported on or before
  that day.
- `market_cap` uses the **current** `shares_outstanding` snapshot from
  `dim_company` (not point-in-time historical shares). Same compromise as
  `dim_company_enriched.market_cap_latest`. Point-in-time shares is a
  Phase C upgrade.
- `dividend_yield_ttm` is the firm-level cash yield
  (dividend_payout_ttm / market_cap), not the per-share dividend yield.
- Elfenbein formula: `fair_pe = growth_pct/2 + 8`. Null when 5y/3y EPS
  CAGR is null or negative (formula breaks for declining EPS). `fair_price`
  is also null when `reported_eps_ttm` ≤ 0.
- Ratios use raw `close`, not `adjusted_close` — these are point-in-time
  valuation metrics, not return calculations.

Cadence: daily (after `build_prices_enriched`). Also rebuilt in the weekly
workflow so the snapshot reflects fresh fundamentals immediately.

### `fact_sector_aggregates` (Phase B)

Long-format aggregate snapshot. One row per
`(as_of_date, grouping_level, group_value, metric)`. Single snapshot per
rebuild (= latest `trade_date` in `fact_valuation_daily`).

| Column | Type | Notes |
|---|---|---|
| as_of_date | DATE | Snapshot date |
| grouping_level | VARCHAR | `'sector'` or `'industry'` |
| group_value | VARCHAR | The sector or industry name |
| metric | VARCHAR | See metric list below |
| n | BIGINT | Group size (number of symbols with a non-null value) |
| mean | DOUBLE | |
| median | DOUBLE | |
| p25 | DOUBLE | |
| p75 | DOUBLE | |
| gold_built_utc | VARCHAR | |

Metrics aggregated:
- Valuation (from `fact_valuation_daily`): pe_ttm, ps_ttm, pb,
  ev_ebitda_ttm, fcf_yield_ttm, dividend_yield_ttm
- Fundamentals (latest quarterly row from `fact_fundamentals_wide`):
  revenue_growth_yoy, eps_growth_yoy, gross_margin, operating_margin,
  net_margin, roe, roic

Cadence: weekly (after `build_fundamentals_wide`).

### `fact_peer_relative` (Phase B)

Per-symbol percentile rank and z-score within sector and industry, for
each metric. Single snapshot per rebuild matching the as-of date in
`fact_sector_aggregates`.

| Column | Type | Notes |
|---|---|---|
| as_of_date | DATE | |
| symbol | VARCHAR | |
| metric | VARCHAR | Same metric list as `fact_sector_aggregates` |
| value | DOUBLE | The symbol's raw metric value |
| sector | VARCHAR | From dim_company |
| industry | VARCHAR | From dim_company |
| sector_percentile | DOUBLE | `PERCENT_RANK()` within sector partition, range [0, 1] |
| industry_percentile | DOUBLE | `PERCENT_RANK()` within industry partition, range [0, 1] |
| sector_zscore | DOUBLE | `(value − sector_mean) / sector_stddev`, null when stddev = 0 |
| industry_zscore | DOUBLE | Same, within industry |
| sector_n | BIGINT | Sector group size — filter to `n ≥ 3` for meaningful medians |
| industry_n | BIGINT | Industry group size — currently mostly 1 at 107-ticker scale; expands meaningfully past 500 tickers |
| gold_built_utc | VARCHAR | |

Cadence: weekly (after `build_sector_aggregates`).

---

## Gold Layer — Extensions (Sentiment, Insider, Institutional, Indices)

### `fact_sentiment_daily`

One row per `(symbol, sentiment_date)` summarizing ticker-level news sentiment
attached to articles published that calendar day. Joinable to
`fact_valuation_daily` on `(symbol, trade_date = sentiment_date)`.

| Column | Type | Notes |
|---|---|---|
| symbol | VARCHAR | |
| sentiment_date | DATE | UTC calendar date of `time_published` |
| article_count | INT | |
| avg_sentiment_score | DOUBLE | Simple average across articles |
| weighted_avg_sentiment | DOUBLE | Weighted by per-article `relevance_score` |
| bullish_count | INT | `ticker_sentiment_label IN ('Bullish','Somewhat-Bullish')` |
| bearish_count | INT | `ticker_sentiment_label IN ('Bearish','Somewhat-Bearish')` |
| gold_built_utc | VARCHAR | |

Partitioning: `year=YYYY/`. Cadence: daily.

### `fact_insider_signals`

One row per `(symbol, as_of_date)` for the trailing 5 years. Rolling 30/90/180-day
net insider buying signals, joinable to valuation/price tables.

| Column | Type | Notes |
|---|---|---|
| symbol | VARCHAR | |
| as_of_date | DATE | Calendar date |
| net_insider_shares_30d | DOUBLE | Σ (A − D) shares within trailing 30d |
| net_insider_shares_90d | DOUBLE | trailing 90d |
| net_insider_shares_180d | DOUBLE | trailing 180d |
| net_insider_usd_30d | DOUBLE | Σ signed (shares × share_price) |
| net_insider_usd_90d | DOUBLE | |
| net_insider_usd_180d | DOUBLE | |
| distinct_buyers_180d | INT | |
| distinct_sellers_180d | INT | |
| cluster_buy_flag_30d | BOOLEAN | True when ≥3 distinct executives bought in trailing 30d |
| gold_built_utc | VARCHAR | |

Cadence: weekly.

### `fact_institutional_concentration`

One row per `(symbol, pull_date)`. Ownership concentration plus snapshot-over-snapshot
deltas (lag based on prior pull within ticker).

| Column | Type | Notes |
|---|---|---|
| symbol | VARCHAR | |
| pull_date | DATE | |
| total_institutional_shares | DOUBLE | Joined from `dim_institutional_summary` |
| top10_pct_owned | DOUBLE | Σ top-10 holders' shares / total_institutional_shares |
| top25_pct_owned | DOUBLE | |
| holder_count | INT | |
| prev_holder_count | INT | Prior snapshot value (LAG) |
| holder_count_delta | INT | Δ vs prior snapshot |
| prev_total_shares | DOUBLE | |
| total_shares_delta | DOUBLE | |
| gold_built_utc | VARCHAR | |

Cadence: weekly.

### `fact_transcript_sentiment`

One row per `(symbol, quarter)` — call-level sentiment aggregated from
`fact_transcript_turns`, split by speaker role so management tone can be
compared against analyst reception. Roles are classified from the free-text
`title` column: contains "operator" → operator (excluded from averages),
contains "analyst" → analyst, anything else with a title → management.

| Column | Type | Notes |
|---|---|---|
| symbol | VARCHAR | |
| quarter | VARCHAR | `YYYYQN` |
| scored_turns | BIGINT | Turns with non-null sentiment |
| mgmt_turns / analyst_turns | BIGINT | |
| overall_avg_sentiment | DOUBLE | Mgmt + analyst turns only (operator excluded) |
| mgmt_avg_sentiment | DOUBLE | Simple average over management turns |
| analyst_avg_sentiment | DOUBLE | |
| ceo_cfo_avg_sentiment | DOUBLE | Management narrowed to C-suite titles |
| mgmt_wtd_sentiment / analyst_wtd_sentiment | DOUBLE | Content-length-weighted averages |
| mgmt_analyst_spread | DOUBLE | mgmt_avg − analyst_avg; compression = mgmt losing conviction |
| call_date | DATE | From `dim_earnings_calls` |
| participant_count | INT | From `dim_earnings_calls` |
| gold_built_utc | VARCHAR | |

Cadence: weekly (after `transform_earnings_transcripts`).

### `dim_peer_sets`

Up to 5 algorithmically selected peers per active stock, for comp-set
analysis (e.g. `notebooks/stock_deep_dive.ipynb`, which also accepts a
manual override list). Selection: same industry preferred (sector backfill
when the industry pool is thin), candidates more than 100x larger/smaller
excluded, ranked by `0.6 × market-cap proximity + 0.4 × trailing-2y
daily-return correlation`.

| Column | Type | Notes |
|---|---|---|
| as_of_date | DATE | Build date |
| symbol | VARCHAR | |
| peer_rank | INT | 1..5 |
| peer_symbol | VARCHAR | |
| peer_name | VARCHAR | |
| industry_match | BOOLEAN | False = sector-level backfill |
| log_cap_ratio | DOUBLE | log10(peer cap / symbol cap) |
| return_corr_2y | DOUBLE | Null when < 126 common trading days |
| score | DOUBLE | Composite ranking score |
| gold_built_utc | VARCHAR | |

Cadence: weekly (after `build_dim_company_enriched`).

### `fact_index_returns`

Parallel to `fact_prices_enriched` but for indices. No volume-based metrics
(indices don't carry volume). Useful as a true SPX benchmark instead of SPY,
or to compute per-ticker beta vs each index.

| Column | Type | Notes |
|---|---|---|
| index_symbol | VARCHAR | |
| trade_date | DATE | |
| open / high / low / close | DOUBLE | Raw OHLC from silver |
| return_1d / 5d / 21d / 63d / 126d / 252d | DOUBLE | From close |
| volatility_30d / 90d | DOUBLE | Annualized stdev of daily returns |
| high_252d / low_252d | DOUBLE | Rolling 252-day extrema |
| pct_off_252d_high | DOUBLE | Distance from 1-year high |
| gold_built_utc | VARCHAR | |

Partitioning: `year=YYYY/`. Cadence: daily.

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
