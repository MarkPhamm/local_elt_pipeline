# dbt Charts Documentation

## 1. Overview

<img src="../images/dbt-charts.png" alt="dbt Charts" style="width:100%">

[dbt Charts](https://docs.dbtcharts.com/) is a YAML language and CLI for declaring interactive dashboards next to dbt models. Boards live in Git, query DuckDB through the existing dbt profile, and render locally with no extra BI server.

This project uses dbt Charts as the code-first BI layer over the CFPB marts.

**Why it fits this stack**:

- Dashboards sit in `duckdb_dbt/charts/` next to the models they query
- SQL uses `{{ ref() }}`, so chart queries track dbt model names and schemas
- `dct serve` / `dct render` run locally against DuckDB
- `dct validate` is cheap CI for YAML, chart fields, and `ref()` resolution

## 2. Installation

dbt Charts is a project dependency:

```bash
uv sync
uv run dct --version
```

Standalone install (optional):

```bash
uv tool install dbt-charts
dct --version
```

Do not install dbt Charts into a dbt v2 environment. This repo uses dbt Core 1.x via `dbt-duckdb`, so the project virtualenv is safe.

## 3. Project Structure

```text
duckdb_dbt/
├── dbt_project.yml
├── profiles.yml              # DuckDB connection reused by dbt Charts
├── dbt_charts.yml            # Sources, server port
├── charts/
│   ├── meta.yml              # Default source and theme
│   ├── overview.yml          # Landing page
│   ├── executive.yml
│   ├── geographic.yml
│   ├── product.yml
│   └── response.yml
└── models/marts/core/        # Tables the boards query
```

`dbt_charts.yml` holds engine knobs (sources, port). Presentation defaults belong in `charts/meta.yml`.

## 4. Data Source

Boards use the `duckdb_cfpb` source, which reads the dbt profile:

```yaml
# duckdb_dbt/dbt_charts.yml
sources:
  duckdb_cfpb:
    type: dbt_profile
    profile: duckdb_dbt
    target: dev
```

That profile points at `database/cfpb_complaints.duckdb`. Queries resolve models with `ref()`:

```yaml
queries:
  company_metrics:
    sql: |
      select company, total_complaints
      from {{ ref('dim_companies') }}
      order by total_complaints desc
      limit 20
```

`ref()` needs `duckdb_dbt/target/manifest.json`. Generate it with `dbt parse` or `dbt run`.

## 5. Boards

| Board | URL | Purpose |
|-------|-----|---------|
| Overview | `/overview/` | Links to the analysis boards |
| Executive | `/executive/` | KPIs, monthly volume vs dispute rate, product mix, top companies |
| Geographic | `/geographic/` | US state map, ranked bar, state table |
| Product | `/product/` | Product donut, volume vs response time, sub-product table |
| Response | `/response/` | Timely-response trend, company scatter, response-type mix |

All four analysis boards query marts (`fct_complaints`, `agg_complaints_by_month`, `dim_companies`, `dim_products`, `dim_states`, `dim_response_types`). They do not scan the raw fact table for aggregates that already exist in dimensions.

## 6. Usage

**Preview** (from repo root):

```bash
cd duckdb_dbt
uv run dbt parse
uv run dct serve
```

Or from the repo root:

```bash
uv run dbt parse --project-dir duckdb_dbt --profiles-dir duckdb_dbt
uv run dct serve --project-dir duckdb_dbt
```

Default URL: <http://localhost:8080>

Board paths map to URLs: `charts/executive.yml` is `/executive/`.

**Validate**:

```bash
uv run dbt parse --project-dir duckdb_dbt --profiles-dir duckdb_dbt
uv run dct validate --project-dir duckdb_dbt
```

Add `--warehouse` to DESCRIBE queries against DuckDB after marts are built.

**Render a static artifact**:

```bash
uv run dct render duckdb_dbt/charts/executive.yml --project-dir duckdb_dbt --format html
uv run dct render duckdb_dbt/charts/executive.yml --project-dir duckdb_dbt --format png --output executive.png
```

## 7. Adding a Chart

1. Query a mart with `ref()` (or reuse an existing named query).
2. Bind columns to a chart type (`line`, `bar`, `donut`, `scatter`, `kpi`, `table`, `map`).
3. Place the chart id in `rows` / `cols`.
4. Run `dct validate --project-dir duckdb_dbt`.

Minimal example:

```yaml
title: "Top companies"

queries:
  top_companies:
    sql: |
      select company, total_complaints
      from {{ ref('dim_companies') }}
      order by total_complaints desc
      limit 10

charts:
  top_companies_bar:
    query: top_companies
    type: bar
    x: company
    y: total_complaints
    style:
      orientation: horizontal

rows:
  - top_companies_bar
```

Dual-axis charts use `layers:` plus `axis_y.position: right` on the overlay. Filled US maps use `type: map`, `geo.source: us-states`, and a lookup key that matches the geography source (this project maps state abbreviations to FIPS).

## 8. Integration with dbt

```text
dbt models (marts layer)
    -> DuckDB (database/cfpb_complaints.duckdb)
    -> dbt Charts queries ({{ ref() }})
    -> Boards in duckdb_dbt/charts/
```

Workflow:

```bash
uv run python run_prefect_flow.py   # load + dbt
cd duckdb_dbt && uv run dct serve   # dashboards
```

After changing a model name or column, re-run `dbt parse` / `dbt run` and `dct validate`. CI runs parse + validate on every pull request.

Mart SQL that ends in `select * from ...` produces `WARN-DBT-MODEL-COLUMNS-UNRESOLVED` during validate. That is expected with the current marts; `dct validate --warehouse` checks columns against DuckDB after `dbt run`.

## 9. Troubleshooting

**`ERR-DBT-MANIFEST-MISSING`**: run `dbt parse` in `duckdb_dbt/` so `target/manifest.json` exists.

**Empty charts**: confirm the DuckDB file exists and marts were built (`cd duckdb_dbt && dbt run`). Test SQL with `dbt show --select dim_companies --limit 10`.

**Database connection failed**: check `duckdb_dbt/profiles.yml` (`path: ../database/cfpb_complaints.duckdb`) and run `dbt debug --project-dir duckdb_dbt --profiles-dir duckdb_dbt`.

**Port in use**: `uv run dct serve --project-dir duckdb_dbt --port 3000`.

**Map regions missing**: `us-states` joins on FIPS, not postal codes. `charts/geographic.yml` maps `dim_states.state` to FIPS before lookup.

## 10. Resources

- [dbt Charts docs](https://docs.dbtcharts.com/)
- [Quick guide](https://docs.dbtcharts.com/quick-guide/)
- [YAML schema](https://docs.dbtcharts.com/reference/yaml-reference/)
- [GitHub](https://github.com/dbt-labs/dbt-charts)
