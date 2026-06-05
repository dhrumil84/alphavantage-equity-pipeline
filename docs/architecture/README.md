# Architecture Docs

Living architecture documentation for the equity data pipeline. Diagrams are written in [Mermaid](https://mermaid.js.org/) so they render natively on GitHub and diff cleanly in PRs.

## Contents

- [overview.md](overview.md) — high-level flow: source → bronze → silver → gold → consumers.
- [data-model.md](data-model.md) — entity-relationship diagram of silver + gold tables.
- [pipeline-dag.md](pipeline-dag.md) — job dependencies for the daily and weekly cron workflows.
- [data-quality.md](data-quality.md) — where DQ checks run, what they gate, where reports land.
- [decisions/](decisions/) — Architecture Decision Records (ADRs). One short file per non-obvious choice.

## Conventions

- **One diagram per concern.** Don't try to fit ingestion, transformation, and DQ into one mega-flowchart.
- **Update with the code.** A PR that changes pipeline structure should update the relevant diagram in the same PR.
- **Use ADRs for the "why".** Diagrams show *what*; ADRs explain *why* we picked it over the alternatives.
- **Prefer Mermaid.** Reach for [draw.io](https://app.diagrams.net/) (export `.drawio` + `.svg`, commit both) only when auto-layout isn't good enough.

## Authoring tips

- VS Code: install **Markdown Preview Mermaid Support** to render in the preview pane.
- GitHub: Mermaid renders automatically inside fenced ` ```mermaid ` blocks.
- Live editor: <https://mermaid.live> is useful for iterating on a tricky diagram before pasting it back.
