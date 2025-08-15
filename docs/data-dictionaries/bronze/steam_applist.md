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

**Format:** JSONL (compressed as .jsonl.gz)

**Path:** `s3://gamelens/bronze/steam/applist/year=YYYY/month=MM/day=DD/`

**Grain:** One record per app per snapshot

## **Schema**

| Field                | Type        | Nullable | Description                                   |
|----------------------|------------|----------|-----------------------------------------------|
| appid                | BIGINT     | NO       | Unique Steam App ID. Primary key.            |
| name                 | STRING     | YES      | Application name (may be empty for some IDs). |
| last_modified        | TIMESTAMP  | NO       | UNIX timestamp of the last modification|
| price_change_number  | INT        | YES      | Price revision number (used for change tracking). |

---

## **Validation Rules**
- `appid` must be unique within each ingestion batch.
- `last_modified` must be within a valid range.
- `name` can be empty (some placeholder or deprecated IDs).

## **Notes**
- This dataset represents the **Steam game catalog only** — filtered at the API level. It excludes DLCs, software, videos, and hardware.
