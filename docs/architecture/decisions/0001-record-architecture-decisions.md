# 0001. Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-06-05

## Context

This is a solo project that I expect to grow in scope (more endpoints, more derived tables, eventually a serving layer). Decisions that feel obvious today — why bronze is immutable, why silver dedupes on a specific key, why we use DuckDB instead of Postgres — will not be obvious in six months. The code shows *what* exists; it does not record *why* the alternatives were rejected.

Without a record, future-me (or a future collaborator) will re-litigate decisions every time the code pushes against an old constraint.

## Decision

Use Architecture Decision Records (ADRs), one Markdown file per decision, stored under `docs/architecture/decisions/`. Follow [Michael Nygard's lightweight format](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions): Context, Decision, Alternatives considered, Consequences.

ADRs are append-only. When a decision is reversed, write a new ADR that supersedes the old one rather than editing history.

## Alternatives considered

- **No formal record, just commit messages.** Rejected: commits explain a *change*, not the broader decision space. They're also hard to find later.
- **A single `ARCHITECTURE.md` doc.** Rejected: it conflates current state with historical reasoning, and editing it in place loses the trail.
- **Wiki / Notion.** Rejected: lives outside the repo, drifts from code, no PR review.

## Consequences

- New non-obvious architectural choices need an ADR before or alongside the PR that implements them.
- The cost is ~10 minutes of writing per decision.
- The benefit is a durable record I can grep, link from diagrams, and hand to a future collaborator.
