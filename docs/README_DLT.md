# Data Ingestion Pipeline

Complete guide for the CFPB consumer complaints data ingestion pipeline.

## 1. Overview

The ingestion pipeline extracts consumer complaint data from the CFPB API and loads it into DuckDB using dlt (data load tool). It supports incremental loading with automatic state management via Prefect orchestration.

**Key Features:**

- Incremental loading (initial load + daily updates)
- Automatic state tracking
- Company-specific filtering
- Error handling and retry logic
- Prefect orchestration with monitoring

## 2. Architecture

### 2.1 Components

```text
apis/
  └── cfpb_api_client.py      # CFPB API client with retry logic and pagination

pipelines/
  └── cfpb_complaints_pipeline.py  # dlt pipeline definition
      ├── extract_complaints()     # dlt resource for data extraction
      ├── create_pipeline()        # Pipeline configuration
      └── DuckDB destination       # Loads to database/cfpb_complaints.duckdb

utils/
  └── state.py                     # State management for incremental loads
      ├── get_last_loaded_date()   # Retrieve last successful load date
      ├── update_last_loaded_date() # Update state after successful load
      └── get_next_load_date()     # Determine date range for next load

orchestration/
  └── cfpb_flows.py            # Prefect flow for incremental pipeline
```

### 2.2 Data Source

CFPB Consumer Complaint Database API

- Base URL: `https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/`
- Documentation: <https://cfpb.github.io/api/ccdb/api.html>
- Rate Limits: Max 10,000 records per request (pagination supported)

## 3. Configuration

Edit `src/cfg/config.py`:

```python
# Start date for initial data load
START_DATE = "2024-01-01"

# Companies to extract data for
COMPANIES = [
    "jpmorgan",
    "bank of america",
]
```

## 4. How It Works

### 4.1 Incremental Loading Logic

**Initial Load (First Run):**

1. No state file exists → loads from `START_DATE` to today
2. Processes all companies from config sequentially
3. Saves `last_loaded_date = today` to `pipeline_state.json` after success

**Incremental Load (Subsequent Runs):**

1. Reads `pipeline_state.json` for last loaded date
2. Calculates date range: `last_loaded_date + 1 day` to today
3. If already up to date (start > end), skips execution
4. Extracts and loads only new data
5. Updates state only if all companies succeed

### 4.2 Data Flow

```text
CFPB API
    ↓
cfpb_api_client.py (pagination, retries, error handling)
    ↓
extract_complaints() (dlt resource - adds metadata, ensures complaint_id)
    ↓
create_pipeline() (configures DuckDB destination, raw schema)
    ↓
DuckDB (database/cfpb_complaints.duckdb)
    └── Schema: raw
    └── Table: cfpb_complaints
    └── Write mode: merge (by complaint_id)
```

### 4.3 State Management

**State File:** `pipeline_state.json`

```json
{
  "last_loaded_date": "2025-11-01",
  "updated_at": "2025-11-01T17:30:00.123456"
}
```

**State Update Rules:**

- Only updated if **all companies** load successfully
- Ensures data consistency across companies
- Prevents partial state updates

## 5. Running the Pipeline

### 5.1 Basic Usage

```bash
# Run incremental pipeline (first run = initial load, subsequent = incremental)
uv run python run_prefect_flow.py

# Reset state to reload from START_DATE
uv run python run_prefect_flow.py --reset-state

# Custom database path
uv run python run_prefect_flow.py --database path/to/database.duckdb
```

### 5.2 Programmatic Usage

```python
from src.orchestration.cfpb_flows import cfpb_complaints_incremental_flow

# Run the flow
result = cfpb_complaints_incremental_flow()
```

### 5.3 Direct dlt Usage (Advanced)

```python
from src.pipelines.cfpb_complaints_pipeline import create_pipeline, extract_complaints

pipeline = create_pipeline()
info = pipeline.run(
    extract_complaints(
        date_received_min="2024-01-01",
        date_received_max="2024-12-31",
        company_name="jpmorgan",
    )
)
```

## 6. Prefect Orchestration

### 6.1 Overview

The pipeline uses Prefect 3.x for workflow orchestration, providing:

- Task tracking and monitoring
- Automatic retry logic
- Centralized logging
- Flow run history

### 6.2 Flow Structure

```text
cfpb-complaints-incremental (Flow)
  └── extract_and_load_complaints (Task)
      └── Per company extraction and loading
```

### 6.3 Prefect UI Setup

**Quick Start:**

```bash
# Start Prefect server
./start_prefect_server.sh
# Or: uv run prefect server start

# Access UI at http://127.0.0.1:4200
```

**Running Server in Background:**

```bash
# Option 1: Background process
uv run prefect server start &

# Option 2: With nohup (persists after terminal closes)
nohup uv run prefect server start > prefect_server.log 2>&1 &

# Option 3: Using tmux
tmux new -s prefect
uv run prefect server start
# Press Ctrl+B then D to detach
```

**Using the UI:**

1. Start Prefect server in one terminal
2. Run flows in another terminal - they appear automatically in the UI
3. Monitor: View flow runs, logs, task details, and execution history

**Troubleshooting:**

- Port conflict: Use `--port 4201` to change port
- Flows not appearing: Ensure server is running before executing flows

### 6.4 Scheduling

**Using cron:**

```bash
# Add to crontab (crontab -e)
0 2 * * * cd /path/to/local_elt_pipeline && uv run python run_prefect_flow.py
```

**Using Prefect Cloud:**

For production, you can use Prefect Cloud (free tier available):

```bash
# Sign up at https://app.prefect.cloud
# Login with API key
uv run prefect cloud login --key YOUR_API_KEY
```

## 7. Technical Details

### 7.1 dlt Pipeline Configuration

```python
@dlt.resource(
    name="cfpb_complaints",
    write_disposition="merge",
    primary_key="complaint_id",
)
```

- **write_disposition="merge"**: Updates existing records, inserts new ones
- **primary_key="complaint_id"**: Used for deduplication
- **dataset_name="raw"**: Schema name in DuckDB

### 7.2 Data Transformation

**Primary Key Generation:**

- Uses API `_id` or `id` field if available
- Falls back to hash generation from complaint data

**Metadata Addition:**

- `_dlt_extracted_at`: Extraction timestamp
- `_dlt_load_id`: Load identifier for tracking

**Schema Inference:** dlt automatically infers schema from data

### 7.3 API Client Features

**Pagination:**

- Handles API pagination automatically (max 10,000 records per request)
- Continues fetching until all data retrieved

**Error Handling:**

- Automatic retry with exponential backoff (3 retries)
- Retries on: 429, 500, 502, 503, 504 status codes
- Proper User-Agent header (required by CFPB API)

**Company Filtering:**

- Uses API search functionality for company-specific queries
- Optimized with `no_aggs=True` for faster responses

### 7.4 Data Schema

The loaded data includes fields such as:

- `complaint_id`: Primary key (unique identifier)
- `date_received`: Date complaint was received
- `company`: Company name
- `product`: Product type
- `issue`: Issue description
- `sub_product`: Sub-product category
- `zip_code`: Consumer zip code
- `state`: Consumer state
- `company_response`: Company's response
- `timely`: Whether response was timely
- `consumer_consent_provided`: Consent flag
- `complaint_what_happened`: Narrative description
- Additional fields as provided by the API

## 8. Troubleshooting

### 8.1 State Management

**Reset State:**

```bash
uv run python run_prefect_flow.py --reset-state
# Or manually:
rm pipeline_state.json
```

**Check Last Loaded Date:**

```bash
cat pipeline_state.json
```

**State File Corrupted:**

- Delete `pipeline_state.json` and run again (will trigger initial load)

### 8.2 Data Verification

**Run Tests:**

```bash
uv run pytest tests/test_verify_data.py
```

**Check Database:**

```bash
# Using DuckDB CLI
duckdb database/cfpb_complaints.duckdb
> SELECT COUNT(*) FROM raw.cfpb_complaints;
> DESCRIBE SELECT * FROM raw.cfpb_complaints LIMIT 0;
```

### 8.3 Common Issues

**Port Already in Use:**

```bash
uv run prefect server start --port 4201
```

**Server Won't Start:**

```bash
# Ensure dependencies are installed
uv sync
```

**Flows Not Appearing in UI:**

- Ensure Prefect server is running before executing flows
- Flows must be executed at least once to appear in UI

## 9. Performance Considerations

- **Initial Load**: May take significant time depending on date range and number of companies
- **Incremental Load**: Typically fast, only loads new days
- **API Rate Limits**: Built-in retry logic handles rate limiting gracefully
- **Database Size**: DuckDB efficiently handles large datasets locally

## 10. Next Steps

After ingestion, the data in the `raw` schema can be:

1. **Transformed** using dbt (transformation layer)
2. **Queried** directly from DuckDB using SQL
3. **Analyzed** using BI tools like Evidence

See [README.md](README.md) for quick start instructions.
