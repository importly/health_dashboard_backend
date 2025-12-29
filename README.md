# Digital Physiologist Backend

A high-performance, local-first ETL engine for Apple Health data, built with Rust.

## Features

-   **Streaming Ingestion:** Efficiently parses large XML exports (500MB+) using streaming logic to keep memory usage low.
-   **Flexible Schema:** Database structure is dynamically managed via `metrics_manifest.toml`. No code changes needed to track new metrics.
-   **Normalized Storage:** Data is normalized into SQLite tables for efficient querying.
-   **Time-Series Aggregation:** Built-in API for hourly, daily, and monthly aggregation of health metrics.
-   **Derived Metrics:** Support for virtual generated columns (e.g., calculating Effort Scores from Heart Rate automatically).

## Prerequisites

-   Rust (latest stable)
-   SQLite3

## Configuration

The system is driven by `metrics_manifest.toml`.

```toml
[tables.biometrics]
columns = [
    { field_name = "heart_rate", hk_identifier = "HKQuantityTypeIdentifierHeartRate", data_type = "REAL", aggregate = "avg" },
    # ... other columns
    # Virtual Column Example:
    { field_name = "effort_score", data_type = "REAL", expression = "heart_rate * 0.1", aggregate = "avg" }
]
```

## API Usage

### 1. Ingest Data
Stream an Apple Health export XML file into the database asynchronously.

**POST** `/ingest`
```json
{
  "file_path": "test_export/export.xml"
}
```

Response:
```json
{
  "message": "Ingestion started in background",
  "job_id": "f12c466c-..."
}
```

### 2. Check Ingestion Status
Track the progress of a background ingestion job.

**GET** `/api/ingest/status/{job_id}`

Response:
```json
{
  "status": "processing",
  "progress": 50000,
  "total": null
}
```

### 3. Ingest External Sources (ECG & GPX)
Scan the configured `electrocardiograms/` and `workout-routes/` folders for new files and import them.

**POST** `/api/import/external`

### 3. Query Raw Data
Fetch raw records from any table (including external sources).

**GET** `/api/data/{table}?limit=100&sort={column}`
- `sort`: (Optional) Column to sort by DESC. Defaults to `start_date`. For routes, use `timestamp`. For ECGs, use `recorded_at`.

### 4. Aggregate Data
Get time-bucketed statistics (avg, sum, min, max, count).

**GET** `/api/aggregate/{table}?bucket={interval}`
- `bucket`: `hour`, `day`, or `month`.

Response:
```json
[
  {
    "time_bucket": "2024-01-01T15:00:00Z",
    "heart_rate": 75.5,
    "step_count": 1200
  }
]
```

### 4. Health Check
Verify service status (useful for Docker/Kubernetes probes).

**GET** `/health`

Response:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T12:00:00Z"
}
```

## Development

Run the server locally:
```bash
cargo run
```

The database `health.db` will be automatically created and migrated on startup.

## Deployment (Docker)

Build the image:
```bash
docker build -t digital-physiologist .
```

Run the container (mounting a volume for data persistence):
```bash
docker run -p 3000:3000 -v $(pwd)/data:/usr/local/bin/data digital-physiologist
```
*Note: Ensure `health.db` path in `Dockerfile` matches your volume mount strategy if you change defaults.*