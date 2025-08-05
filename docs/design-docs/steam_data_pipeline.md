# Steam Data Collection & Update Strategy

## Problem Statement

Steam’s game catalog contains **over 130,000 applications**, but API endpoints **do not provide full metadata or metrics in bulk**:

- `GetAppList` (or `IStoreService/GetAppList`) returns only AppIDs and names.
- **Detailed data** (release date, reviews, price, tags, etc.) requires **separate `appdetails` requests per game** due to API design and rate limits.
- **Batch requests:** Only `price_overview` is supported in batch mode; all other details require individual requests.
- **Strict rate limits:** 200 requests per 5 minutes.
- **Limit:** 100,000 requests per day.

**Naive approaches** (updating all games daily) are infeasible due to API limitations and cost.

---

## Solution Overview

GameLens employs a **hybrid, score‑ and change‑driven prioritization system**:

- **Initial sync:** A full catalog load (all games) over the first two days.
- **Incremental updates:** Use the `last_modified` field from Steam API to track new or changed games, significantly reducing daily update volume.
- **Score‑based prioritization:** Focuses updates on trending, popular, and new games, regardless of modification date.
- **Resource efficiency:** Avoids wasteful API calls for inactive or irrelevant titles.
- **Dynamic queues:** Ensures new games and hot titles are refreshed frequently, while old inactive games are deprioritized.

---

## Implementation

### Data Source & Filtering

- **Catalog loading:**
  - Use `IStoreService/GetAppList` to retrieve the full list of AppIDs and names.
  - Update catalog periodically (e.g., weekly) as new games appear.
- **Filtering pipeline:**
  - **Initial pass:** Remove software, DLC, videos, hardware using API flags if available.
  - **Secondary pass:** For each AppID, fetch minimal details and **filter only those with `type=game`**.
  - **Deduplicate** and assign a persistent `core_game_id` for cross-source analytics.

### Catalog Size

- Total catalog: **~138,000 unique games** (as of 2025‑08).
- Focus for analytics: **Top several thousand** games (based on score and/or recency).

### API Request Strategy

- **Initial sync:**
  - Fetch all games (`appdetails`) over ~2 days (≈70k/day).
  - Populate the master dataset.
- **Incremental updates:**
  - Use the `last_modified` field to fetch only games modified since the last update.
  - Average daily changes: **~40–1,100 games/day** (peaks during sales or major updates).
- **Price data:** Batch requests allowed (`filters=price_overview`, up to 100 AppIDs per call).
- **Score‑based refresh:** Independently update high‑scoring games (even if `last_modified` is old).
- **Respect rate limits:** 200 requests / 5 min (~40 req/min), max 100k/day.
- **Retries:** Use exponential backoff on 429/500 errors.

### Score‑ and Change‑Based Prioritization

For each game, calculate:

`score = w1 * total_reviews + w2 * reviews_growth_7d + w3 * max(0, 60 - days_since_release)`

(Weights balance popularity, growth, and recency.)

**Update levels:**
- **Level A (“Hot”):**
  - Top ~2,000 by score and all new releases (<60 days).
  - **Update:** Daily.
- **Level B (“Active”):**
  - Games with medium score or recent growth.
  - **Update:** Every 3–7 days.
- **Level C (“Cold”):**
  - Low‑score, inactive, long‑unchanged titles.
  - **Update:** Rarely (1–2 times per year, or if promoted by score/changes).

**Dynamic behavior:** Games are automatically promoted or demoted between levels based on `last_modified` changes and score evolution.

---

## Update Optimization

- **Primary trigger:** Use `last_modified` to detect new or updated games.
- **Score fallback:** Independently refresh top‑scoring games, even without recent modifications.
- **Control group:** Periodically refresh a random sample of “cold” games (e.g., 500 per week) to validate that `last_modified` correctly reflects updates.
- **Full refresh:** Once per year, refresh the entire catalog to ensure no data drift.
- **Peak handling:** If daily changes exceed 5,000 (e.g., during large sales), queue updates over multiple days to stay within limits.

**Estimated load:**
- **Typical:** ~5,000–10,000 API calls/day (within rate limits).
- **Peak events:** Spread over multiple days.
- **Naive:** 100,000+ calls/day (infeasible).

---

## Storage & Data Model

- **Bronze Layer:** Raw API JSON (partitioned in S3 by date/source).
- **Silver Layer:** Cleaned, validated daily snapshots (with score, core_game_id, last_modified).
- **Schema:** See [data-dictionaries/steam_metrics.md](../data-dictionaries/steam_metrics.md)

---

## Validation & Monitoring

- **Validation:** Great Expectations — schema checks, nulls, duplicates, growth sanity.
- **Monitoring:** Pipeline status, API error rates, data freshness ([monitoring/data_quality_checks.md](../monitoring/data_quality_checks.md)).
- **Change detection validation:** Use control group updates to ensure `last_modified` reliability.
- **Retries:** Exponential backoff on 429/rate limit/API errors.
