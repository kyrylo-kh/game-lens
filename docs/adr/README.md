# ADR (Architecture Decision Records)

This directory contains **Architecture Decision Records** (ADRs): short documents capturing key architectural and technical decisions made in this project.

---

## Why ADRs?

- **Document context:** Explain why and when particular architectural decisions were made.
- **Onboarding:** Help new team members quickly understand the reasoning behind critical choices.
- **Change tracking:** Preserve the history of decisions, including those that are changed or reversed.
- **Transparency:** All important decisions are documented and open for review.

---

## When to create an ADR

- When choosing or switching a core technology or service (e.g., database, cloud, framework).
- When making significant changes to architecture or workflows.
- When defining important processes (ETL, monitoring, CI/CD, etc).

---

## Naming Convention

- Use the format: `NNN-title-with-dashes.md`, where `NNN` is a sequential number (e.g., `004-new-etl-framework.md`).

---

## Recommended ADR Template

```md
# [Title of Decision]

- **Date:** YYYY-MM-DD
- **Status:** [Proposed | Accepted | Deprecated | Superseded]
- **Context:**
  [Briefly describe the problem or requirement.]
- **Decision:**
  [What decision was made and why?]
- **Alternatives:**
  [What alternatives were considered? Why weren't they chosen?]
- **Consequences:**
  [What are the pros/cons? What is affected by this decision?]
```