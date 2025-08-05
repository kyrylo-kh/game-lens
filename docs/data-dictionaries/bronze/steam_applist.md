# Bronze: Steam AppList

**Layer:** Bronze

**Source:** [Steam API — IStoreService/GetAppList](https://api.steampowered.com/IStoreService/GetAppList/v1/)

**Request parameters:**
```
key=<api_key>
include_games=true
include_dlc=false
include_software=false
include_videos=false
include_hardware=false
max_results=50000
last_appid=<cursor>
```

**Example:**
```
https://api.steampowered.com/IStoreService/GetAppList/v1/?key=<api_key>&include_games=true&include_dlc=false&include_software=false&include_videos=false&include_hardware=false&max_results=50000&last_appid=2805040
```

**Format:** Parquet (Snappy)

**Path:** `s3://gamelens/bronze/steam/applist/year=YYYY/month=MM/day=DD/`

## **Schema**

| Field                | Type        | Nullable | Description                                   |
|----------------------|------------|----------|-----------------------------------------------|
| appid                | INT        | NO       | Unique Steam App ID. Primary key.            |
| name                 | STRING     | YES      | Application name (may be empty for some IDs). |
| last_modified        | TIMESTAMP  | NO       | UNIX timestamp of the last modification|
| price_change_number  | INT        | YES      | Price revision number (used for change tracking). |
| ingest_date          | DATE       | NO       | **Added by ETL.** Date when the dataset was ingested into the data lake. |

---

## **Validation Rules**
- `appid` must be unique within each ingestion batch.
- `last_modified` must be within a valid range.
- `name` can be empty (some placeholder or deprecated IDs).

## **Notes**
- This dataset represents the **Steam game catalog only** — filtered at the API level. It excludes DLCs, software, videos, and hardware.
- `ingest_date` is added by the ingestion pipeline for partitioning and audit purposes. It is not part of the Steam API payload."
