# Steam Data Collection & Update Strategy

## Problem Statement

Steam’s game catalog contains tens of thousands of applications, but API endpoints **do not provide full metadata or metrics in bulk**:

- `GetAppList` (or `IStoreService/GetAppList`) returns only AppIDs and names.
- **Detailed data** (release date, reviews, price, tags, etc.) requires **separate `appdetails` requests per game** due to API design and rate limits.
- **Batch requests:** Only `price_overview` is supported in batch mode; all other details require individual requests.
- **Strict rate limits:** 200 requests per 5 minutes.

**Naive approaches** (updating all games daily) are infeasible due to API limitations and cost.

---

## Solution Overview

GameLens employs a **dynamic, score-based prioritization system**:

- Focuses updates on trending, popular, and new games.
- Minimizes wasteful API calls for inactive titles.
- Stays within Steam’s strict rate limits while maintaining analytics freshness.

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

- Final catalog: **10,000–20,000 unique games** after filtering.

### API Request Strategy

- **Detailed data:** Only single-app requests supported (`appdetails` per AppID).
- **Price data:** Batch requests allowed (`filters=price_overview`, up to 100 AppIDs per call).
- **Respect rate limits:** 200 requests / 5 min (~40 req/min).
- **Caching:** Skip unchanged games to reduce calls.
- **Retries:** Use exponential backoff on 429/500 errors.

### Score-Based Prioritization

For each game, calculate:

`score = w1 * total_reviews + w2 * reviews_growth_7d + w3 * max(0, 60 - days_since_release)`

(Weights balance popularity, growth, and recency.)

- **Level A (“Hot”):** Top ~2,000 by score and all new releases (<60 days).
  **Update:** Daily
- **Level B (“Medium”):** Middle tier.
  **Update:** Every 3 days
- **Level C (“Cold”):** Low-score/inactive.
  **Update:** Every 1–2 weeks (spread out)
- **Automatic promotion/demotion** as scores change.

### Update Optimization

- **Level B/C:** Skip update if `last_updated` is unchanged.
- **Full refresh:** Once per month, refresh all games regardless of activity.
- **Estimated load:**
  - Typical: ~5,000–6,000 API calls/day (within rate limits).
  - Naive: 20,000+ calls/day (not feasible).

---

## Storage & Data Model

- **Bronze Layer:** Raw API JSON (partitioned in S3 by date/source).
- **Silver Layer:** Cleaned, validated daily snapshots (with score, core_game_id).
- **Schema:** See [data-dictionaries/steam_metrics.md](../data-dictionaries/steam_metrics.md)

---

## Validation & Monitoring

- **Validation:** Great Expectations — schema checks, nulls, duplicates, growth sanity.
- **Monitoring:** Pipeline status, API error rates, data freshness ([monitoring/data_quality_checks.md](../monitoring/data_quality_checks.md)).
- **Retries:** Exponential backoff on 429/rate limit/API errors.

