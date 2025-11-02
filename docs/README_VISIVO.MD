# Visivo Documentation

## 1. Overview

Visivo is a code-first business intelligence (BI) tool that allows you to build interactive dashboards and visualizations using YAML configuration files. It's designed for data teams who prefer infrastructure-as-code approaches and want version-controlled analytics.

**Key Features**:

- **Code-First**: Define dashboards, charts, and data models in YAML
- **Version Control**: All configurations can be committed to git
- **DuckDB Integration**: Native support for querying DuckDB databases
- **Plotly-Based**: Uses Plotly.js for interactive visualizations
- **Local Development**: Run dashboards locally without cloud dependencies
- **Lightweight**: No separate server infrastructure needed

## 2. Why Visivo for This Project?

### 2.1 Advantages

1. **Fits the Local ELT Philosophy**: Matches the project's approach of local-first development
2. **Configuration as Code**: Dashboards are defined in `project.visivo.yml`, version-controlled alongside data models
3. **DuckDB Native**: Direct integration with our DuckDB data warehouse
4. **Fast Iteration**: Quick feedback loop for dashboard development
5. **No Additional Infrastructure**: Runs on your laptop, no cloud setup required
6. **Data Model Integration**: Works seamlessly with dbt marts layer

## 3. Installation

Visivo is included in the project dependencies via `pyproject.toml`:

```bash
# Install all project dependencies (includes visivo)
uv sync
```

For standalone installation:

```bash
pip install visivo
```

Verify installation:

```bash
visivo --version
```

## 4. Project Structure

```text
visivo/
└── project.visivo.yml    # Main configuration file
```

The configuration file defines:

- **Sources**: Database connections (DuckDB)
- **Models**: SQL queries that fetch data
- **Traces**: Individual visualization components (lines, bars, etc.)
- **Tables**: Data table displays
- **Charts**: Complete charts combining traces and layouts
- **Dashboards**: Full dashboard pages with multiple visualizations

## 5. Configuration Overview

### 5.1 Data Source

The project connects to the DuckDB database:

```yaml
sources:
  - name: duckdb_cfpb
    type: duckdb
    database: ../database/cfpb_complaints.duckdb
```

### 5.2 Data Models

Seven data models query the marts layer:

| Model | Description | Source Table |
|-------|-------------|--------------|
| `company_metrics` | Top 20 companies by complaints | `raw_marts.dim_companies` |
| `monthly_trends` | Time-series metrics by month | `raw_marts.agg_complaints_by_month` |
| `product_breakdown` | Product/sub-product statistics | `raw_marts.dim_products` |
| `state_distribution` | Geographic distribution | `raw_marts.dim_states` |
| `issue_analysis` | Issue/sub-issue breakdown | `raw_marts.dim_issues` |
| `response_types` | Response type effectiveness | `raw_marts.dim_response_types` |
| `kpi_summary` | High-level KPIs | `raw_marts.fct_complaints` |

### 5.3 Visualizations

The configuration includes various chart types:

- **Bar Charts**: Company complaint volumes
- **Line Charts**: Monthly trends (complaints, dispute rates, timely responses)
- **Pie Charts**: Product distribution
- **Choropleth Maps**: Geographic heat maps
- **Scatter Plots**: Response time vs complaint volume analysis
- **Data Tables**: Detailed tabular data

## 6. Available Dashboards

### 6.1 Executive Dashboard (`executive_dashboard`)

**Purpose**: High-level overview for leadership and decision-makers

**Components**:

- **KPI Cards**: Total complaints, companies, dispute rate, average response time
- **Monthly Trends**: Dual-axis chart showing complaints volume and dispute rate
- **Product Distribution**: Pie chart of complaints by product category
- **Top Companies**: Bar chart and detailed table of top 20 companies
- **Company Performance Table**: Sortable table with key metrics

**Best For**:

- Executive reporting
- Quick status checks
- Identifying problem areas

**Access**: Primary landing page

### 6.2 Geographic Analysis Dashboard (`geographic_dashboard`)

**Purpose**: Understand complaint distribution across US states

**Components**:

- **US Heat Map**: Choropleth map showing complaint volume by state
- **State Statistics Table**: Detailed metrics for top 25 states
  - Total complaints
  - Dispute percentage
  - Most common product
  - Most common issue

**Best For**:

- Regional analysis
- Identifying geographic patterns
- State-specific investigations
- Market expansion decisions

### 6.3 Product Analysis Dashboard (`product_dashboard`)

**Purpose**: Deep dive into product and sub-product performance

**Components**:

- **Product Distribution**: Pie chart of top-level product categories
- **Response Time Analysis**: Scatter plot showing relationship between complaint volume and response time
- **Product Details Table**: Top 30 product/sub-product combinations with:
  - Complaint volume
  - Dispute percentage
  - Average response time
  - Most common associated issue

**Best For**:

- Product team insights
- Issue prioritization
- Product improvement initiatives

### 6.4 Response Performance Dashboard (`response_dashboard`)

**Purpose**: Analyze company response effectiveness and timeliness

**Components**:

- **Timely Response Trend**: Line chart showing percentage of timely responses over time
- **Response Time Scatter**: Bubble chart colored by dispute rate
- **Company Performance Table**: Metrics on response quality

**Best For**:

- Operations teams
- SLA monitoring
- Customer service optimization
- Identifying response bottlenecks

## 7. Usage

### 7.1 Running Visivo

**Start the Visivo server**:

```bash
# Navigate to the visivo directory
cd visivo

# Compile and run queries (does not start web server)
uv run visivo run

# Start web server to view dashboards
uv run visivo serve

# Or from project root with working directory flag
cd /path/to/local_elt_pipeline
uv run visivo run --working-dir visivo
```

**Alternative ports**:

```bash
# Use custom port
cd visivo
uv run visivo run --port 3000
```

**Open in browser**:

```text
http://localhost:8080
```

### 7.2 Navigation

Once Visivo is running:

1. **Dashboard List**: Main page shows all available dashboards
2. **Dashboard Selection**: Click on any dashboard name to view
3. **Interactive Charts**:
   - Hover over data points for details
   - Click and drag to zoom
   - Double-click to reset zoom
   - Use toolbar for pan, zoom, export
4. **Tables**: Click column headers to sort

### 7.3 Exporting Visualizations

Visivo charts can be exported directly from the UI:

1. Hover over any chart
2. Click the camera icon in the toolbar
3. Choose format (PNG, SVG, etc.)
4. Download to your computer

### 7.4 Building Static Sites

Generate a static HTML site:

```bash
# Build static site from visivo directory
cd visivo
uv run visivo dist --output-dir ../dashboard_build

# Or from project root
uv run visivo dist --working-dir visivo --output-dir ./dashboard_build

# Serve static site
cd dashboard_build
python -m http.server 8000
```

This creates a fully static dashboard that can be:

- Hosted on any web server
- Shared via file system
- Deployed to GitHub Pages, Netlify, etc.

## 8. Customizing Dashboards

### 8.1 Adding a New Chart

1. **Create a data model** in `project.visivo.yml`:

```yaml
models:
  - name: my_new_model
    source: duckdb_cfpb
    sql: |
      SELECT
        column1,
        column2,
        COUNT(*) as total
      FROM raw_marts.my_table
      GROUP BY column1, column2
```

2. **Create a trace** (visualization component):

```yaml
traces:
  - name: my_trace
    model: my_new_model
    props:
      type: bar  # or line, scatter, pie, etc.
      x: query(column1)
      y: query(total)
      name: "My Data"
```

3. **Create a chart** (combine trace with layout):

```yaml
charts:
  - name: my_chart
    traces:
      - my_trace
    layout:
      title: "My New Chart"
      xaxis:
        title: Column 1
      yaxis:
        title: Total
```

4. **Add to dashboard**:

```yaml
dashboards:
  - name: executive_dashboard
    rows:
      - height: large
        items:
          - width: 12
            chart: my_chart  # Reference your new chart
```

### 8.2 Modifying Existing Visualizations

**Change colors**:

```yaml
traces:
  - name: my_trace
    props:
      marker:
        color: "#FF6B6B"  # Use hex colors
```

**Adjust chart size**:

```yaml
dashboards:
  - name: my_dashboard
    rows:
      - height: large  # small, medium, large, xlarge
        items:
          - width: 6  # 1-12 (bootstrap grid)
            chart: my_chart
```

**Modify SQL queries**:
Simply edit the SQL in the model definition:

```yaml
models:
  - name: company_metrics
    source: duckdb_cfpb
    sql: |
      SELECT
        company,
        total_complaints,
        pct_disputed
      FROM raw_marts.dim_companies
      WHERE total_complaints > 100  -- Add filters
      ORDER BY total_complaints DESC
      LIMIT 50  -- Change limit
```

## 9. Common Chart Types

### 9.1 Bar Chart

```yaml
traces:
  - name: bar_trace
    model: my_model
    props:
      type: bar
      x: query(category)
      y: query(value)
      marker:
        color: "#3B82F6"
```

### 9.2 Line Chart

```yaml
traces:
  - name: line_trace
    model: my_model
    props:
      type: scatter
      mode: lines+markers
      x: query(date)
      y: query(value)
      line:
        color: "#10B981"
        width: 2
```

### 9.3 Pie Chart

```yaml
traces:
  - name: pie_trace
    model: my_model
    props:
      type: pie
      labels: query(category)
      values: query(value)
```

### 9.4 Scatter Plot

```yaml
traces:
  - name: scatter_trace
    model: my_model
    props:
      type: scatter
      mode: markers
      x: query(x_value)
      y: query(y_value)
      text: query(label)
      marker:
        size: 10
        color: query(color_value)
        colorscale: Viridis
```

### 9.5 Choropleth Map (US States)

```yaml
traces:
  - name: map_trace
    model: my_model
    props:
      type: choropleth
      locations: query(state)  # Two-letter state codes
      z: query(value)
      locationmode: USA-states
      colorscale: Blues
```

## 10. Integration with dbt

Visivo dashboards are designed to work with dbt marts:

```text
dbt models (marts layer)
    ↓
DuckDB database
    ↓
Visivo data models (SQL queries)
    ↓
Visivo visualizations
    ↓
Interactive dashboards
```

**Workflow**:

1. Run dbt to build marts:

```bash
cd duckdb_dbt
dbt run
```

2. Start Visivo to visualize:

```bash
cd ../visivo
uv run visivo serve
```

3. Make changes to dbt models, re-run dbt, refresh Visivo browser

## 11. Best Practices

### 11.1 Data Model Design

**Keep queries focused**:

```yaml
# Good: Specific, focused query
models:
  - name: top_companies
    sql: |
      SELECT company, total_complaints
      FROM raw_marts.dim_companies
      ORDER BY total_complaints DESC
      LIMIT 20

# Avoid: Overly broad queries
models:
  - name: everything
    sql: SELECT * FROM raw_marts.fct_complaints  # Too much data
```

### 11.2 Performance

**Use aggregated tables**:

- Query `raw_marts.dim_*` and `raw_marts.agg_*` instead of `raw_marts.fct_complaints`
- Pre-aggregate data in dbt models
- Use `LIMIT` clauses for large datasets

**Example**:

```yaml
# Good: Query aggregated dimension
SELECT * FROM raw_marts.dim_companies LIMIT 20

# Avoid: Aggregating in Visivo
SELECT company, COUNT(*)
FROM raw_marts.fct_complaints
GROUP BY company
```

### 11.3 Dashboard Organization

- **One focus per dashboard**: Executive, Geographic, Product, etc.
- **Top-to-bottom flow**: KPIs → Trends → Details
- **Consistent colors**: Use brand colors across all charts
- **Meaningful titles**: Clear, descriptive chart titles

### 11.4 Version Control

**Commit changes**:

```bash
git add visivo/project.visivo.yml
git commit -m "Add new product analysis chart"
git push
```

**Review changes**:

```bash
git diff visivo/project.visivo.yml
```

## 12. Troubleshooting

### 12.1 Issue: Dashboard not loading

**Error**: `Database connection failed`

**Solution**: Ensure DuckDB database exists and dbt models have been run:

```bash
# Check database exists
ls -lh database/cfpb_complaints.duckdb

# Run dbt to create marts
cd duckdb_dbt
dbt run
```

### 12.2 Issue: Chart shows no data

**Error**: Empty chart or "No data" message

**Solution**: Test the SQL query directly in DuckDB:

```bash
duckdb database/cfpb_complaints.duckdb

# Test your model's SQL
SELECT * FROM raw_marts.dim_companies LIMIT 10;
```

### 12.3 Issue: Visivo command not found

**Error**: `visivo: command not found`

**Solution**: Ensure you're using `uv run` and running from the correct directory:

```bash
# From visivo directory:
cd visivo
uv run visivo run

# Or from project root with working directory flag:
uv run visivo run --working-dir visivo
```

### 12.4 Issue: Port already in use

**Error**: `Port 8080 is already in use`

**Solution**: Use a different port:

```bash
cd visivo
uv run visivo serve --port 3000
```

## 13. Advanced Features

### 13.1 Dynamic Text with Variables

Use model data in text blocks:

```yaml
dashboards:
  - name: my_dashboard
    rows:
      - height: small
        items:
          - width: 12
            text: |
              ## Total Complaints
              **{{kpi_summary.total_complaints}}**

              As of {{kpi_summary.last_updated}}
```

### 13.2 Multiple Traces on One Chart

Combine multiple data series:

```yaml
charts:
  - name: multi_line_chart
    traces:
      - complaints_line
      - dispute_rate_line
      - timely_rate_line
    layout:
      title: "Multiple Metrics Over Time"
```

### 13.3 Dual Y-Axis Charts

Show different scales:

```yaml
traces:
  - name: trace1
    props:
      yaxis: y
  - name: trace2
    props:
      yaxis: y2  # Second y-axis

charts:
  - name: dual_axis
    traces: [trace1, trace2]
    layout:
      yaxis:
        title: "Complaints"
      yaxis2:
        title: "Percentage"
        overlaying: y
        side: right
```

## 14. Comparison with Other BI Tools

| Feature | Visivo | Metabase | Tableau |
|---------|--------|----------|---------|
| **Deployment** | Local/Code | Self-hosted/Cloud | Desktop/Cloud |
| **Configuration** | YAML files | UI-based | UI-based |
| **Version Control** | Native | Limited | None |
| **Cost** | Free/Open Source | Free (Basic) | Paid |
| **Learning Curve** | Medium | Low | High |
| **Customization** | High (code) | Medium (UI) | High (UI) |
| **DuckDB Support** | Native | Via JDBC | Limited |
| **Best For** | Data engineers | Business users | Analysts |

## 15. Project-Specific Notes

### 15.1 Current Dashboard Setup

The project includes **4 pre-configured dashboards**:

1. **Executive Dashboard**: Primary overview, start here
2. **Geographic Dashboard**: State-by-state analysis
3. **Product Dashboard**: Product and sub-product breakdown
4. **Response Dashboard**: Company response performance

### 15.2 Data Refresh

Dashboards automatically reflect current data:

```bash
# Update data pipeline
python run_prefect_flow.py

# Data flows through:
# 1. dlt → raw tables
# 2. dbt → marts tables
# 3. Visivo → dashboards (auto-refresh on page reload)
```

### 15.3 Customization Points

Common customizations for this project:

1. **Add more companies**: Edit `src/cfg/config.py`, run pipeline
2. **Change date range**: Modify dbt models to filter by date
3. **Add new metrics**: Create calculated fields in dbt, expose in Visivo models
4. **Custom branding**: Update color schemes in traces

## 16. Additional Resources

- **Official Documentation**: [https://visivo.io/docs](https://visivo.io/docs)
- **GitHub Repository**: [https://github.com/visivo-io/visivo](https://github.com/visivo-io/visivo)
- **Examples**: [https://visivo.io/examples](https://visivo.io/examples)
- **Plotly Charts**: [https://plotly.com/python/](https://plotly.com/python/) (underlying library)
- **YAML Syntax**: [https://yaml.org/spec/1.2/spec.html](https://yaml.org/spec/1.2/spec.html)

## 17. Quick Reference

### 17.1 Start Visivo

```bash
cd visivo
uv run visivo serve

# Or from project root:
uv run visivo serve --working-dir visivo
```

### 17.2 Build Static Site

```bash
cd visivo
uv run visivo dist --output-dir ../dashboard_build

# Or from project root:
uv run visivo dist --working-dir visivo --output-dir ./dashboard_build
```

### 17.3 Compile Project (Check for errors)

```bash
cd visivo
uv run visivo compile
```

### 17.4 View Project Structure

```bash
cd visivo
uv run visivo compile

# This will show you all models, charts, and dashboards
# Or check the project.visivo.yml file directly
```

### 17.5 Full Pipeline Refresh

```bash
# 1. Run ELT pipeline (from project root)
python run_prefect_flow.py

# 2. Start dashboards web server
cd visivo
uv run visivo serve

# 3. Open browser
open http://localhost:8080
```
