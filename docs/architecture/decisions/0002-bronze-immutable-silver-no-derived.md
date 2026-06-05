# 0002. Bronze is immutable; silver holds no derived metrics (one exception)

- **Status:** Accepted
- **Date:** 2026-06-05

## Context

Alpha Vantage's responses are the only ground truth we have. The API can change shape, return `"None"` strings, omit fields, or quietly correct historical values between pulls. We also expect to make parsing mistakes — both today and as we add endpoints. If the only copy of the source data lives in cleaned tables, every parser bug means re-hitting a rate-limited API and risking a *different* response than the one we originally cleaned.

Separately, the silver layer is the read target for every analytical query, notebook, and (eventually) gold build. If silver mixes raw source columns with derived metrics (ratios, growth rates, returns), three things go wrong:

1. We can't recompute derived metrics independently — the silver schema becomes the cache invalidation problem.
2. Reviewers can't tell which columns are "what the API said" vs "what we inferred."
3. A subtle bug in a derived formula contaminates downstream gold tables silently.

## Decision

Two related rules:

**Bronze is write-once, append-only.** Raw API payloads are stored in `bronze/{endpoint}/{symbol}/{pull_date}.json` and are never modified or deleted, even when we discover a parsing bug. Reprocessing always means *re-reading existing bronze files*, never re-pulling from the API unless we genuinely need a newer snapshot.

**Silver contains cleaned source data only — no derived metrics.** The transforms strip `"None"` to NULL, parse dates, dedupe on documented keys, and stop. Ratios, returns, valuation multiples, growth rates, peer comparisons all live in the gold layer (`transform_gold/*`) or in notebooks.

**One exception:** `fact_cash_flow.free_cash_flow` is computed in silver as `operating_cashflow - abs(capex)`. The carve-out is narrow and justified: it's a same-row arithmetic combination of two columns we already have, Alpha Vantage doesn't return it reliably, and every downstream consumer wants it. If we ever find ourselves arguing for a second exception, that's the signal to revisit this ADR.

## Alternatives considered

- **Mutable bronze ("just overwrite on re-pull").** Rejected: loses the audit trail. When a parser bug surfaces, we'd need the original bytes, not a fresher response that may have changed.
- **Skip bronze entirely; parse straight to silver.** Rejected: every parsing change costs an API round-trip (and burns our 75/min budget). Bronze is cheap R2 storage; re-hitting the API isn't.
- **Allow derived metrics in silver freely.** Rejected: silver becomes a junk drawer, gold becomes redundant, and we lose the ability to recompute derived fields without re-touching source-of-truth tables.
- **No silver exceptions at all (push free_cash_flow to gold).** Considered seriously. Rejected because (a) it's trivially derivable from same-row columns, (b) every fundamentals consumer wants it, and (c) the alternative is duplicating the formula in every gold builder. The exception is documented and narrow.

## Consequences

- Bronze grows unboundedly. We accept the storage cost — R2 is cheap and the audit value is high. `observability/storage_scan` tracks growth so we'll notice if this assumption breaks.
- Parser changes require a one-time backfill from existing bronze, but no API quota spend.
- Gold builders own all derived metrics. New ratios go in `transform_gold/`, not in silver transforms.
- A new request for a silver derived column triggers an ADR — we don't extend the exception list silently.
- If the API ever issues true corrections (not just snapshots) for historical data, we need a deliberate policy for which bronze pull "wins." Out of scope here.
