# Local ELT pipeline

A local-first ELT pipeline that extracts CFPB consumer complaint data, transforms it with dbt into analytics-ready models, and serves interactive dashboards—all running on your laptop with zero cloud dependencies.

* **Package Manager**: uv (Python)
* **Ingestion**: dlt
* **Transformation**: dbt core
* **OLAP Database**: DuckDB
* **Orchestration**: Prefect
* **BI Tool**: Visivo

<img src="https://github.com/user-attachments/assets/1b4e8d7b-0527-4fdc-b104-562cf0c3efa6" alt="Architecture Diagram" style="width: 100%; height: auto;" />

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

### 1.4 Access Prefect UI (Optional)

```bash
# Start Prefect server
./start_prefect_server.sh

# Access UI at http://127.0.0.1:4200
```
<img width="2562" height="1352" alt="image" src="https://github.com/user-attachments/assets/81c72031-f948-455c-8dc2-d7b2483ce747" />

### 1.5 Access DuckDB UI

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

Additional docs: [DuckDB UI Documentation](https://duckdb.org/docs/api/cli/ui)

### 1.6 Access Visivo Dashboards

Start the Visivo web server to view interactive dashboards:

```bash
# Navigate to visivo directory
cd visivo

# Start web server (runs on http://localhost:8080 by default)
uv run visivo serve

# Or use a custom port
uv run visivo serve --port 3000
```

Then open your browser to:

* **Dashboard URL**: <http://localhost:8080>

<img width="2390" height="601" alt="image" src="https://github.com/user-attachments/assets/e87eb339-7514-467a-a01a-860582b6bab4" />


**Available Dashboards**:

* **Executive Dashboard**: High-level overview of complaint trends and company performance
* **Geographic Dashboard**: State-by-state complaint distribution and analysis

**Note**: Before viewing dashboards, ensure:

1. Data pipeline has been run (`uv run python run_prefect_flow.py`)
2. dbt models have been built (`cd duckdb_dbt && dbt run`)

Additional docs: [Visivo Documentation](docs/README_VISIVO.MD)

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
