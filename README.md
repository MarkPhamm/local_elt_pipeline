# Local Data Warehouse

A local-first data warehouse pipeline that extracts CFPB consumer complaint data, transforms it with dbt into analytics-ready models, and serves interactive dashboards—all running on your laptop with zero cloud dependencies.

* **Package Manager**: [uv](https://docs.astral.sh/uv/) (Python)
* **Ingestion**: [dlt](docs/README_DLT.md) + [PyArrow](docs/PYARROW.md) (API → Parquet staging → DuckDB)
* **Staging Format**: [Parquet](docs/PYARROW.md) (via PyArrow) in `landing/`
* **Transformation & Documentation**: [dbt core](docs/README_DBT.md) & [dbt-colibri](docs/README_DBT.md)
* **OLAP Database**: [DuckDB](docs/README_DUCKDB.md)
* **Orchestration**: [Prefect](docs/README_PREFECT.md)
* **BI Tool**: [dbt Charts](docs/README_DBT_CHARTS.md)

<img src="images/architecture.png" alt="Architecture diagram: CFPB to dltHub to DuckDB and dbt to dbt Charts. Prefect orchestrates the pipeline; GitHub Actions CI covers linting and dbt Charts validation." style="width: 100%; height: auto;" />

## 1. Quick Start

### 1.1 Setup

```bash
# Install dependencies
uv sync

# Install dev dependencies (for testing)
uv sync --extra dev
```

### 1.2 Configuration

Edit `src/cfg/config.py` to configure companies and start date:

```python
START_DATE = "2024-01-01"
COMPANIES = ["jpmorgan", "bank of america"]
```

### 1.3 Run the Pipeline

```bash
# Run incremental pipeline (first run loads from START_DATE to today)
uv run python run_prefect_flow.py

# Reset state to reload from START_DATE
uv run python run_prefect_flow.py --reset-state
```

### 1.4 Backfill Landing Area

The landing area stores daily parquet files under `landing/cfpb_complaints/YYYY_MM_DD/`, one file per company per day. Use the backfill script to populate historical data:

```bash
# Backfill a date range
uv run python run_backfill.py --start 2026-01-01 --end 2026-02-25

# Backfill last 7 days
uv run python run_backfill.py --days 7

# Backfill a single day
uv run python run_backfill.py --start 2026-01-15 --end 2026-01-15
```

Each daily directory contains 10 parquet files (one per company). Empty parquet files are created for days with no complaints to maintain a consistent structure.

```
landing/cfpb_complaints/
├── 2026_01_01/
│   ├── jpmorgan_2026-01-01_2026-01-02.parquet
│   ├── bank_of_america_2026-01-01_2026-01-02.parquet
│   └── ...
├── 2026_01_02/
│   └── ...
```

### 1.5 Access Prefect UI (Optional)

```bash
# Start Prefect server
./start_prefect_server.sh

# Access UI at http://127.0.0.1:4200
```

<img width="2562" height="1352" alt="image" src="https://github.com/user-attachments/assets/81c72031-f948-455c-8dc2-d7b2483ce747" />

### 1.6 Access DuckDB UI

First, install the DuckDB CLI:

```bash
# macOS (using Homebrew)
brew install duckdb

# Or download from https://duckdb.org/docs/installation/
```

Then launch the DuckDB UI:

```bash
duckdb -ui
```

We need to add in our database path as follow:
<img width="2566" height="1352" alt="image" src="https://github.com/user-attachments/assets/0167dae6-0e77-42b9-ae4f-09bfe2490b5c" />
<img width="2554" height="1353" alt="image" src="https://github.com/user-attachments/assets/743c707f-d2a4-4fe4-a76e-829a88c6a3ec" />

Additional docs: [DuckDB UI Documentation](https://duckdb.org/docs/api/cli/ui)

### 1.7 Access dbt Charts Dashboards

Boards live next to the dbt models they query. Parse (or run) dbt so `ref()` can resolve, then serve:

```bash
uv run dbt parse --project-dir duckdb_dbt --profiles-dir duckdb_dbt
uv run dct serve --project-dir duckdb_dbt
```

Then open your browser to:

* **Dashboard URL**: <http://localhost:8080>

**Available boards**:

* **Overview**: Links to the analysis boards
* **Executive**: KPIs, monthly trends, product mix, and top companies
* **Geographic**: US state map and ranked state statistics
* **Product**: Product mix, response time, and sub-product detail
* **Response**: Timeliness trends, company scatter, and response types

**Note**: Before viewing dashboards, ensure:

1. Data pipeline has been run (`uv run python run_prefect_flow.py`)
2. dbt models have been built (`cd duckdb_dbt && dbt run`)

Additional docs: [dbt Charts Documentation](docs/README_DBT_CHARTS.md)

### 1.8 Generate dbt Lineage Reports with Colibri

Generate interactive data lineage reports to visualize how data flows through your dbt models:

```bash
# Navigate to the dbt project directory
cd duckdb_dbt

# Compile models and generate dbt documentation
dbt compile && dbt docs generate

# Generate lineage report
colibri generate
```

Open `duckdb_dbt/dist/index.html` in your browser to explore the interactive lineage visualization showing model dependencies, data flow, and column-level lineage.

Additional docs: [dbt Documentation](docs/README_DBT.md)

## 2. Testing

```bash
# Run python tests
uv run pytest tests/
```

```bash
# Run dbt tests
cd duckdb_dbt
dbt test
```

## 3. License

See [LICENSE](LICENSE) file for details.
