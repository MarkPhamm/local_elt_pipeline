# CFPB Complaints Pipeline – Workflow Overview

---

## 1. Flow Entry

**Function:** `cfpb_complaints_incremental_flow()`
**File:** `src/orchestration/cfpb_flows.py`

This is the main Prefect flow. It:

- Determines the load window using `get_next_load_date()`  
  → **File:** `src/utils/state.py`
- Reads `COMPANIES` from config  
  → **File:** `src/cfg/config.py`
- Loops through each company and calls the Extract+Load task

---

## 2. Extract to Parquet (per company)

**Task:** `extract_to_parquet_task()`
**File:** `src/orchestration/cfpb_flows.py`

Inside this task:

### ✔ `save_to_parquet()`

**File:** `src/pipelines/cfpb_complaints_pipeline.py`
Calls the CFPB API via `extract_complaints()` and writes results as a Parquet file to the landing area:
`landing/cfpb_complaints/{company}_{date_min}_{date_max}.parquet`

This step handles **Extract → Stage**.

---

## 3. Load Parquet to DuckDB (per company)

**Task:** `load_parquet_to_duckdb_task()`
**File:** `src/orchestration/cfpb_flows.py`

Inside this task:

### ✔ `load_parquet_to_duckdb()`

**File:** `src/pipelines/cfpb_complaints_pipeline.py`
Reads the staged Parquet file and loads it into DuckDB via dlt (preserving merge/dedup logic):
`database/cfpb_complaints.duckdb`

This step handles **Stage → Load**.

---

## 4. Update Incremental State

If all companies completed successfully:

### ✔ `update_last_loaded_date(date_max)`

**File:** `src/utils/state.py`

Updates `pipeline_state.json`, enabling:

- Incremental loading on next run
- Avoiding reprocessing old data

---

## 5. Transform with dbt

**Task:** `run_dbt_models_task()`  
**File:** `src/orchestration/cfpb_flows.py`

Process SQL models by layers

## 6. Test with dbt

**Task:** `run_dbt_tests_task()`
**File:** `src/orchestration/cfpb_flows.py`

---

## 7. Serve dashboards with dbt Charts

After marts are built, boards in `duckdb_dbt/charts/` query them with `ref()`:

```bash
uv run dbt parse --project-dir duckdb_dbt --profiles-dir duckdb_dbt
uv run dct serve --project-dir duckdb_dbt
```

Open <http://localhost:8080>. See [dbt Charts Documentation](README_DBT_CHARTS.md).
