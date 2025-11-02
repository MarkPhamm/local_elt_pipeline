# DuckDB Documentation

## 1. Overview

DuckDB is an in-process SQL OLAP (Online Analytical Processing) database management system used as the data warehouse for this project. It combines the portability of SQLite with the analytical performance of traditional data warehouses.

**Key Features**:

- **In-Process**: Runs embedded within applications, no separate server needed
- **Columnar Storage**: Optimized for analytical queries (aggregations, GROUP BY, JOIN)
- **ACID Compliant**: Full transactional support
- **SQL Support**: Standard SQL with analytical functions (window functions, CTEs, etc.)
- **Zero Dependencies**: Single binary with no external dependencies
- **Fast**: Vectorized query execution engine optimized for OLAP workloads

## 2. Why DuckDB for This Project?

### 2.1 Advantages

1. **Local Development**: Perfect for local ELT pipelines, no need for cloud infrastructure
2. **Performance**: Excellent analytical query performance for datasets up to billions of rows
3. **Simplicity**: Single file database (`cfpb_complaints.duckdb`), easy to backup and version
4. **dlt Integration**: Native support as a dlt destination
5. **dbt Integration**: Well-supported via `dbt-duckdb` adapter
6. **Cost-Effective**: No licensing fees, runs on your laptop
7. **Portability**: Database file can be easily shared or moved

### 2.2 When to Use DuckDB

**Good For**:

- Local analytics and data science workflows
- Development and testing environments
- Datasets that fit in memory or on disk (up to terabytes)
- Analytical queries (aggregations, time-series, OLAP)
- Prototyping data pipelines before moving to production

**Not Ideal For**:

- High-concurrency transactional workloads (OLTP)
- Multi-user write access (use PostgreSQL, MySQL instead)
- Real-time operational databases
- Applications requiring distributed architecture

## 3. Architecture

### 3.1 Storage Architecture

```text
┌────────────────────────────────────────────────────────────┐
│                  DuckDB Architecture                       │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│              Application Layer                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │   dlt    │  │   dbt    │  │  Python  │                  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘                  │
│       │             │             │                        │
│       └─────────────┴─────────────┘                        │
│                    │                                       │
└────────────────────┼───────────────────────────────────────┘
                     │
┌────────────────────┼───────────────────────────────────────┐
│                    │        DuckDB SQL Engine              │
│                    │  • Query Parser & Optimizer           │
│                    │  • Vectorized Execution Engine        │
│                    │  • Transaction Manager (MVCC)         │
└────────────────────┼───────────────────────────────────────┘
                     │
┌────────────────────┼───────────────────────────────────────┐
│                    │    Storage Layer (Columnar)           │
│                    │  • Data stored in compressed columns  │
│                    │  • Min/Max indices for pruning        │
│                    │  • Statistics for query optimization  │
└────────────────────┼───────────────────────────────────────┘
                     │
┌────────────────────┼───────────────────────────────────────┐
│                    │         File System                   │
│                    │    database/cfpb_complaints.duckdb    │
│                    │      (Single database file)           │
└────────────────────┴───────────────────────────────────────┘
```

### 3.2 How It Works in This Project

```text
┌────────────────────────────────────────────────────────────┐
│                       Data Flow                            │
└────────────────────────────────────────────────────────────┘

1. Data Ingestion (dlt)
   └─> Writes to: cfpb_complaints.duckdb
       Schema: raw.cfpb_complaints (raw table)

2. Data Transformation (dbt)
   └─> Reads from: raw schema
       Writes to:
       ├─> staging.stg_cfpb__complaints (view)
       ├─> intermediate.int_cfpb__complaint_metrics (view)
       └─> marts schema (tables):
           ├─> marts.fct_complaints
       ├─> intermediate.int_cfpb__complaint_metrics (view)
       └─> marts schema (tables):
           ├─> marts.fct_complaints
           ├─> marts.dim_companies
           ├─> marts.dim_products
           ├─> marts.dim_issues
           ├─> marts.dim_states
           ├─> marts.dim_response_types
           └─> marts.agg_complaints_by_month

3. Data Analysis
   └─> Query marts tables using:
       ├─> DuckDB CLI
       ├─> DuckDB UI (Web Interface)
       ├─> Python (duckdb library)
       └─> BI Tools (Visivo, Metabase, etc.)

```

### 3.3 Schema Organization

The database is organized into three schemas following the medallion architecture:

| Schema | Type | Purpose | Materialization |
|--------|------|---------|-----------------|
| `raw` | Bronze | Raw data from dlt ingestion | Table |
| `staging` | Silver | Cleaned, standardized data | View |
| `intermediate` | Silver | Business logic applied | View |
| `marts` | Gold | Analytics-ready models | Table |

## 4. Installation

### 4.1 Installing DuckDB CLI

**macOS**:

```bash
# Using Homebrew
brew install duckdb

# Verify installation
duckdb --version
```

**Linux**:

```bash
# Download binary
wget https://github.com/duckdb/duckdb/releases/download/v1.1.0/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
sudo mv duckdb /usr/local/bin/

# Verify installation
duckdb --version
```

**Windows**:

```powershell
# Using winget
winget install DuckDB.cli

# Or download from https://duckdb.org/docs/installation/
```

### 4.2 Installing DuckDB Python Library

The Python library is already included in this project's dependencies via `pyproject.toml`:

```bash
# Install project dependencies (includes duckdb)
uv sync
```

For standalone installation:

```bash
pip install duckdb
```

## 5. Using DuckDB

### 5.1 DuckDB CLI

Connect to the database and run queries:

```bash
# Open the database
duckdb database/cfpb_complaints.duckdb

# Or launch CLI and attach database
duckdb
```

**Basic Commands**:

```sql
-- Show all schemas
SHOW SCHEMAS;

-- Show tables in a schema
SHOW TABLES;

-- Describe table structure
DESCRIBE raw.cfpb_complaints;

-- Query data
SELECT * FROM marts.dim_companies LIMIT 10;

-- Show table sizes
SELECT
    table_schema,
    table_name,
    pg_size_pretty(pg_total_relation_size(table_schema || '.' || table_name)) as size
FROM information_schema.tables
WHERE table_schema IN ('raw', 'staging', 'intermediate', 'marts')
ORDER BY pg_total_relation_size(table_schema || '.' || table_name) DESC;

-- Exit
.quit
```

**Useful CLI Commands**:

```sql
-- Enable timing for queries
.timer on

-- Show query execution plan
EXPLAIN SELECT * FROM marts.fct_complaints;

-- Export results to CSV
COPY (SELECT * FROM marts.dim_companies) TO 'companies.csv' (HEADER, DELIMITER ',');

-- Import CSV into table
CREATE TABLE my_table AS SELECT * FROM read_csv_auto('file.csv');

-- Show database size
SELECT pg_size_pretty(pg_database_size(current_database()));
```

### 5.2 DuckDB UI (Web Interface)

DuckDB includes a built-in web-based UI for exploring and querying data.

**Launch the UI**:

```bash
# Open database with UI
duckdb database/cfpb_complaints.duckdb -ui

# Or launch UI without specific database
duckdb -ui
```

This will:

1. Start a local web server (typically on `http://localhost:8080`)
2. Automatically open your browser to the UI
3. Allow you to explore schemas, run queries, and visualize results

**Features of DuckDB UI**:

- **Schema Browser**: Visual tree view of all tables and columns
- **Query Editor**: SQL editor with syntax highlighting and auto-completion
- **Results Viewer**: Tabular view of query results with sorting and filtering
- **Data Visualization**: Basic charts and graphs for query results
- **Export**: Download results as CSV, JSON, or Parquet
- **Query History**: View and rerun previous queries

**Example Queries to Try in UI**:

```sql
-- Explore the data
SELECT * FROM marts.dim_companies ORDER BY total_complaints DESC LIMIT 10;

-- Time series analysis
SELECT
    complaint_month_date,
    total_complaints,
    avg_days_to_response
FROM marts.agg_complaints_by_month
ORDER BY complaint_month_date;

-- Geographic distribution
SELECT
    state,
    total_complaints,
    pct_disputed,
    most_common_product
FROM marts.dim_states
ORDER BY total_complaints DESC
LIMIT 20;

-- Product hierarchy
SELECT
    product,
    sub_product,
    total_complaints,
    pct_disputed
FROM marts.dim_products
WHERE sub_product IS NOT NULL
ORDER BY product, total_complaints DESC;
```

**UI Documentation**: [https://duckdb.org/docs/api/cli/ui](https://duckdb.org/docs/api/cli/ui)

### 5.3 Python Integration

Query DuckDB from Python scripts:

```python
import duckdb

# Connect to database
conn = duckdb.connect('database/cfpb_complaints.duckdb', read_only=True)

# Run query
result = conn.execute("""
    SELECT company, total_complaints, pct_disputed
    FROM marts.dim_companies
    ORDER BY total_complaints DESC
    LIMIT 10
""").fetchdf()

print(result)
conn.close()
```

**With pandas integration**:

```python
import duckdb
import pandas as pd

# Query directly to DataFrame
df = duckdb.query("""
    SELECT * FROM marts.fct_complaints
    WHERE complaint_year = 2024
""").to_df()

# Register pandas DataFrame as table
duckdb.register('my_df', df)
result = duckdb.query("SELECT * FROM my_df WHERE state = 'CA'").to_df()
```

**Context manager pattern**:

```python
import duckdb

with duckdb.connect('database/cfpb_complaints.duckdb') as conn:
    # Run multiple queries
    conn.execute("SELECT COUNT(*) FROM raw.cfpb_complaints")
    print(conn.fetchone())

    # Use relation API
    rel = conn.table('marts.dim_companies')
    result = rel.filter('total_complaints > 1000').order('total_complaints DESC').limit(5).df()
    print(result)
```

### 5.4 Integration with dbt

The `dbt-duckdb` adapter allows dbt to run transformations on DuckDB:

```bash
cd duckdb_dbt

# Run models
dbt run

# Run specific model
dbt run --select dim_companies

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

Configuration (`duckdb_dbt/profiles.yml`):

```yaml
duckdb_dbt:
  outputs:
    dev:
      type: duckdb
      path: ../database/cfpb_complaints.duckdb
      threads: 1
      schema: raw
  target: dev
```

## 6. Performance Optimization

### 6.1 Best Practices

1. **Use Appropriate Data Types**:

   ```sql
   -- Good: Specific types
   CREATE TABLE my_table (
       id INTEGER,
       date DATE,
       amount DECIMAL(10,2)
   );

   -- Avoid: Generic types
   CREATE TABLE my_table (
       id VARCHAR,
       date VARCHAR,
       amount VARCHAR
   );
   ```

2. **Leverage Columnar Storage**:
   - DuckDB automatically stores data in columns
   - Queries selecting few columns are much faster
   - Use `SELECT specific_columns` instead of `SELECT *`

3. **Partitioning for Large Tables**:

   ```sql
   -- Create partitioned table
   CREATE TABLE complaints_partitioned (
       complaint_id INTEGER,
       date_received DATE,
       company VARCHAR
   ) PARTITION BY RANGE (date_received);
   ```

4. **Use CTEs and Views**:
   - Intermediate calculations as CTEs improve readability
   - Views avoid data duplication
   - dbt models leverage this pattern

5. **Statistics and Indices**:

   ```sql
   -- DuckDB automatically maintains statistics
   -- Check table statistics
   SELECT * FROM duckdb_tables() WHERE table_name = 'fct_complaints';
   ```

### 6.2 Query Optimization Tips

```sql
-- Use EXPLAIN to understand query plans
EXPLAIN ANALYZE SELECT * FROM marts.fct_complaints WHERE state = 'CA';

-- Filter early in query
-- Good: Filter before JOIN
SELECT c.*, d.company_name
FROM (SELECT * FROM complaints WHERE date_received > '2024-01-01') c
JOIN dim_companies d ON c.company = d.company;

-- Push aggregations down
-- Good: Aggregate before JOIN when possible
WITH complaint_counts AS (
    SELECT company, COUNT(*) as cnt
    FROM complaints
    GROUP BY company
)
SELECT d.*, c.cnt
FROM dim_companies d
JOIN complaint_counts c ON d.company = c.company;
```

## 7. Backup and Maintenance

### 7.1 Backing Up the Database

**Simple file copy**:

```bash
# Stop all connections first
cp database/cfpb_complaints.duckdb database/cfpb_complaints_backup_$(date +%Y%m%d).duckdb
```

**Export to Parquet** (recommended for archival):

```sql
-- Export all tables to Parquet
COPY marts.fct_complaints TO 'backup/fct_complaints.parquet' (FORMAT PARQUET);
COPY marts.dim_companies TO 'backup/dim_companies.parquet' (FORMAT PARQUET);
```

### 7.2 Database Maintenance

```sql
-- Checkpoint to write changes to disk
CHECKPOINT;

-- Vacuum to reclaim space
VACUUM;

-- Analyze tables for statistics
ANALYZE;
```

## 8. Troubleshooting

### 8.1 Common Issues

**Issue**: Database file locked

```text
Error: database is locked
```

**Solution**: Ensure only one connection has write access. Close other connections or use `read_only=True` in Python.

**Issue**: Out of memory

```text
Error: Out of Memory Error
```

**Solution**:

```sql
-- Limit memory usage
SET memory_limit='4GB';
SET temp_directory='/path/to/large/disk';
```

**Issue**: Slow queries
**Solution**:

```sql
-- Enable profiling
PRAGMA enable_profiling;
PRAGMA profiling_output='query_profile.json';

-- Run your query
SELECT ...;

-- View profile
SELECT * FROM pragma_last_profiling_output();
```

### 8.2 Debugging Tips

```sql
-- Show DuckDB version
SELECT version();

-- Show current settings
SELECT * FROM duckdb_settings();

-- Show query progress
SET enable_progress_bar=true;

-- Show detailed errors
SET debug_mode=true;
```

## 9. Comparison with Other Databases

| Feature | DuckDB | SQLite | PostgreSQL |
|---------|--------|--------|------------|
| **Use Case** | OLAP Analytics | OLTP Transactions | General Purpose |
| **Performance** | Fast aggregations | Fast point queries | Balanced |
| **Deployment** | Embedded | Embedded | Client-Server |
| **Concurrency** | Read-heavy | Single writer | Multi-user |
| **Data Size** | GBs to TBs | MBs to GBs | GBs to TBs |
| **SQL Standard** | High | Medium | Very High |
| **Dependencies** | None | None | Server required |

## 10. Additional Resources

- **Official Documentation**: [https://duckdb.org/docs/](https://duckdb.org/docs/)
- **GitHub Repository**: [https://github.com/duckdb/duckdb](https://github.com/duckdb/duckdb)
- **Python API Docs**: [https://duckdb.org/docs/api/python/overview](https://duckdb.org/docs/api/python/overview)
- **dbt-duckdb Adapter**: [https://github.com/duckdb/dbt-duckdb](https://github.com/duckdb/dbt-duckdb)
- **SQL Reference**: [https://duckdb.org/docs/sql/introduction](https://duckdb.org/docs/sql/introduction)
- **Performance Guide**: [https://duckdb.org/docs/guides/performance/overview](https://duckdb.org/docs/guides/performance/overview)

## 11. Project-Specific Usage

### 11.1 Database Location

Make sure that the database is in this location

```text
database/cfpb_complaints.duckdb  (662MB in current state)
```

### 11.2 Schemas in Use

```sql
-- View all schemas and tables
SELECT
    table_schema,
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema IN ('raw', 'staging', 'intermediate', 'marts')
ORDER BY table_schema, table_name;
```

### 11.3 Quick Health Check

```sql
-- Row counts across layers
SELECT 'raw' as layer, COUNT(*) as row_count FROM raw.cfpb_complaints
UNION ALL
SELECT 'marts - fact', COUNT(*) FROM marts.fct_complaints
UNION ALL
SELECT 'marts - companies', COUNT(*) FROM marts.dim_companies
UNION ALL
SELECT 'marts - products', COUNT(*) FROM marts.dim_products
UNION ALL
SELECT 'marts - issues', COUNT(*) FROM marts.dim_issues
UNION ALL
SELECT 'marts - states', COUNT(*) FROM marts.dim_states
UNION ALL
SELECT 'marts - responses', COUNT(*) FROM marts.dim_response_types
UNION ALL
SELECT 'marts - monthly', COUNT(*) FROM marts.agg_complaints_by_month;
```

### 11.4 Performance Monitoring

```sql
-- Check query execution time
.timer on
SELECT state, COUNT(*) FROM marts.fct_complaints GROUP BY state;

-- Show table sizes
SELECT
    table_schema || '.' || table_name as full_table_name,
    estimated_size as row_count,
    pg_size_pretty(SUM(pg_column_size(*))) as approx_size
FROM information_schema.tables t
JOIN information_schema.columns c USING (table_schema, table_name)
WHERE table_schema IN ('raw', 'marts')
GROUP BY table_schema, table_name, estimated_size
ORDER BY estimated_size DESC;
```
