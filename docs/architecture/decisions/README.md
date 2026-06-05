# Architecture Decision Records

Short, append-only records of non-obvious architectural choices. Inspired by [Michael Nygard's ADR format](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions).

## Why bother

Diagrams show *what*. ADRs explain *why* — and crucially, why the alternatives were rejected. Six months from now, when a feature pushes against a current constraint, the ADR tells you whether the constraint is load-bearing or just historical.

## Rules

- **One decision per file.** Numbered: `0001-short-title.md`, `0002-...`.
- **Append-only.** Don't rewrite history. Supersede a decision with a new ADR that links back.
- **Short.** A page or less. If it's longer, you're explaining the implementation, not the decision.
- **Write them when the decision is fresh.** A week later you'll have forgotten the alternatives you considered.

## When to write one

Write an ADR when you make a choice that:
- Constrains future work (picks a tool, a layering rule, a schema convention).
- Has a non-obvious trade-off (the runner-up alternative was tempting).
- Would surprise a future reader looking at just the code.

Skip ADRs for: routine code changes, bug fixes, things already documented in `PROJECT_BRIEF.md`.

## Index

- [0001-record-architecture-decisions.md](0001-record-architecture-decisions.md) — Use ADRs.
- [0002-bronze-immutable-silver-no-derived.md](0002-bronze-immutable-silver-no-derived.md) — Bronze is immutable; silver holds no derived metrics (one exception: `free_cash_flow`).

## Template

Copy [_template.md](_template.md) for new entries.
