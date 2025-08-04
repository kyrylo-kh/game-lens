# Core Game Catalog

The **Core Game Catalog** is the unified list of all video games tracked by GameLens, used as the reference entity table across the entire analytics platform.

- **Purpose:**
  - Assigns a persistent `core_game_id` to every unique game, enabling reliable joins between Steam, YouTube, Twitch, etc.
  - Deduplicates games with multiple platform IDs or alternate titles.
  - Provides a single source of truth for game identity in all downstream analytics.

- **Update Logic:**
  - Rebuilt daily as a full snapshot by merging the latest raw source data (Steam, etc.) with the previous version.
  - New games are added; attributes for existing games are updated as needed.
  - `core_game_id` never changes for an existing game, even if external IDs (like Steam AppID) are updated.

- **Storage:**
  - Latest version stored as Parquet files in AWS S3.
  - Available in ClickHouse for fast analytics queries.

- **Fields:**
  - See [data-dictionaries/core_game_catalog.md](../data-dictionaries/core_game_catalog.md) for schema and details.

- **Usage:**
  - All Silver/Gold tables, metrics, and BI dashboards join via `core_game_id`.

- **Notes:**
  - Non-game entries and unresolved records are excluded.
  - Ambiguous cases (e.g. rebranded titles or ID changes) are resolved automatically where possible; manual review is possible if needed.
  - Only the latest snapshot is considered the truth for analytics; previous versions can be archived for auditing if required.
