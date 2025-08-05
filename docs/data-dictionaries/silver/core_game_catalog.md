# Silver: Core Game Catalog

**Layer:** Silver

**Purpose:**
The **Core Game Catalog** is the unified, authoritative list of all games tracked by GameLens.

It assigns a persistent `core_game_uuid` to each unique game, consolidates identifiers from multiple sources (Steam, YouTube, Twitch, etc.), and provides a single source of truth for game identity across all downstream analytics.

**Storage:**
- **Format:** Parquet (Snappy)
- **Path:** `s3://gamelens/silver/core_game_catalog/snapshot_date=YYYY-MM-DD/`
- **Versioning:** All daily snapshots are retained, allowing historical analysis and point-in-time recovery.

**Update frequency:**
- **Initial load:** Full catalog build using Steam AppList + AppDetails.
- **Incremental updates:** Daily — only new or changed games are merged based on `last_modified` and scoring rules.
- **Snapshots:** A full updated snapshot is written **only when changes are detected**.

---

## **Schema**

| Field                 | Type             | Nullable | Description                                      |
|-----------------------|------------------|----------|--------------------------------------------------|
| core_game_uuid        | STRING (UUIDv5)  | NO       | **Stable deterministic UUID.** Generated from normalized name + primary source ID. |
| canonical_name        | STRING           | NO       | Normalized primary game name.                    |
| alternate_names       | ARRAY<STRING>    | YES      | Other known titles / aliases.                    |
| primary_source        | STRING           | NO       | Source where the game was first discovered (e.g., Steam). |
| steam_appid           | INT              | YES      | Steam AppID (nullable if no match).              |
| youtube_game_tag_id   | STRING           | YES      | YouTube game tag ID (nullable).                  |
| twitch_game_id        | STRING           | YES      | Twitch game ID (nullable).                       |
| release_date          | DATE             | YES      | Release date (if available).                     |
| genres                | ARRAY<STRING>    | YES      | Normalized list of genres.                       |
| platforms             | ARRAY<STRING>    | YES      | Supported platforms (PC, Xbox, PS, etc.).        |
| created_at            | TIMESTAMP        | NO       | When this `core_game_uuid` was first generated.  |
| updated_at            | TIMESTAMP        | NO       | When this record was last updated.               |
| snapshot_date         | DATE             | NO       | Snapshot date — also used as a partition key.    |

---

## **Notes**
- **`core_game_uuid`** is generated as a **deterministic UUID v5** using `lower(trim(canonical_name)) + ":" + primary_source_id`. This ensures stability across updates and deduplication across sources.
- The catalog represents **a complete snapshot of the state of all tracked games** as of `snapshot_date`.
- **New games:** Assigned a new `core_game_uuid` when first seen.
- **Updated games:** Attributes (name, genres, platforms) are refreshed when changes are detected.
- **No-change days:** If no games are added or updated, no new snapshot is written for that date.

---

## **Validation Rules**
- `core_game_uuid` must be unique across the catalog.
- `canonical_name` cannot be null or empty.
- `primary_source` must be one of the predefined source codes (`steam`, `youtube`, `twitch`, ...).
- `release_date` must be a valid date if present.
