# Bronze: Steam Game Details

**Layer:** Bronze

**Source:** [Steam API — AppDetails](https://store.steampowered.com/api/appdetails?appids=<id>)

**Request parameters:**
- `appids` — individual Steam AppID (one request per game).

**Format:** Parquet (Snappy)

**Path:** `s3://gamelens/bronze/steam/appdetails/year=YYYY/month=MM/day=DD/`

---

## **Schema**

| Field               | Type              | Nullable | Description                             |
|---------------------|-------------------|----------|-----------------------------------------|
| appid               | INT               | NO       | Steam App ID. Primary key.             |
| success             | BOOLEAN           | NO       | Indicates if the API returned valid data. |
| data_raw            | STRING            | YES      | Raw JSON payload from API (entire `data` object). |
| ingest_date         | DATE              | NO       | **Added by ETL.** Date of ingestion into the data lake. |

---

## **Notes**
- The `data_raw` field contains the **entire raw JSON structure** from the API response (nested fields preserved as a single JSON string).
- This dataset intentionally **does not flatten nested objects** (e.g., genres, categories, screenshots, platforms). These are processed later in the Silver layer.
- **`ingest_date`** is added by the ETL process for partitioning and audit purposes (not part of the Steam API).

---

## **Validation Rules**
- `appid` must be unique within each ingestion batch.
- `success` must be `true` for valid records. Failed API calls are logged separately.
- `data_raw` can be null if `success=false` (invalid or removed app).
