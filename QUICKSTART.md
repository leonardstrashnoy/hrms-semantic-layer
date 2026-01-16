# Quick Start Guide

Get your HRMS semantic layer running in minutes - **no data transfer required!**

## Overview

This semantic layer uses DuckDB to query your SQL Server database directly through federated queries. No data is copied to your local machine unless you explicitly cache specific views.

## Prerequisites

1. **Python 3.8+** installed
2. **ODBC Driver** for SQL Server:
   - **Windows**: Usually pre-installed
   - **macOS**: `brew install unixodbc msodbcsql17`
   - **Linux**: Follow [Microsoft's guide](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
3. **Network access** to SQL Server at `192.168.20.203:1433`

## Installation Steps

### 1. Install Python dependencies

```bash
cd /Users/leonardstrashnoy/Documents/Projects/hrms-semantic-layer
pip install -r requirements.txt
```

### 2. Configure database connection

Create `config.yaml` from the example:

```bash
cp config.example.yaml config.yaml
```

Edit `config.yaml` and add your SQL Server credentials:

```yaml
sql_server:
  host: "192.168.20.203"
  port: 1433
  database: "dbo"
  username: "YOUR_USERNAME"     # ← Change this
  password: "YOUR_PASSWORD"     # ← Change this
  driver: "ODBC Driver 17 for SQL Server"
```

### 3. Initialize the semantic layer

```bash
python init_semantic_layer.py
```

This creates:
- DuckDB database file (`hrmsdb.duckdb`)
- All schemas and views
- Metadata tables
- **No data is transferred!**

### 4. Test the connection

```bash
python scripts/test_connection.py
```

Should show:
- ✓ ODBC extension loaded
- ✓ Connection configured
- ✓ Successfully connected to SQL Server

### 5. Run example queries

```bash
python scripts/query_example.py
```

This demonstrates various queries against the semantic layer.

## Usage

### Python API

```python
import duckdb

# Connect to semantic layer
conn = duckdb.connect('hrmsdb.duckdb')

# Query employees (hits SQL Server)
employees = conn.execute("""
    SELECT full_name, department, job_title, annual_salary
    FROM business.employee_summary
    WHERE employment_status = 'Active'
    ORDER BY annual_salary DESC
    LIMIT 10
""").fetchdf()

print(employees)
```

### Available Views

**Business Views** (user-friendly):
- `business.employee_summary` - Complete employee information
- `business.payroll_detail` - Payroll with context
- `business.attendance_detail` - Attendance with calculations

**Metrics** (aggregated):
- `metrics.monthly_payroll_metrics` - Monthly payroll by department
- `metrics.headcount_metrics` - Employee counts by dimension
- `metrics.attendance_metrics` - Attendance rates and trends

**Staging** (cleaned raw data):
- `staging.stg_employees`
- `staging.stg_payroll`
- `staging.stg_attendance`
- `staging.stg_activity_log`

## Performance Optimization

### Option 1: Cache frequently accessed views

For views you query often (e.g., for dashboards):

```bash
# Cache a specific view
python scripts/cache_view.py business.employee_summary

# Now query the cached version (much faster!)
```

```python
# Use cached data instead of live SQL Server
conn.execute("SELECT * FROM cache.employee_summary").fetchdf()
```

### Option 2: Use metrics instead of details

Metrics are pre-aggregated and query less data:

```python
# Instead of this (slow):
conn.execute("""
    SELECT department, COUNT(*), AVG(salary)
    FROM business.employee_summary
    GROUP BY department
""")

# Use this (faster):
conn.execute("""
    SELECT department, active_count, avg_annual_salary
    FROM metrics.headcount_metrics
    WHERE employment_status = 'Active'
""")
```

## Customization

### Add a new business view

1. Create a SQL file in `models/business/`:

```sql
-- models/business/my_new_view.sql
CREATE OR REPLACE VIEW business.my_new_view AS
SELECT
    e.full_name,
    e.department,
    p.total_earnings
FROM staging.stg_employees e
LEFT JOIN (
    SELECT employee_id, SUM(gross_pay) as total_earnings
    FROM staging.stg_payroll
    GROUP BY employee_id
) p ON e.employee_id = p.employee_id;
```

2. Re-run initialization:

```bash
python init_semantic_layer.py
```

Your new view is now available!

## Troubleshooting

### "ODBC Driver not found"

Install the ODBC driver:
- **macOS**: `brew install msodbcsql17`
- **Linux**: See [Microsoft's guide](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)

### "Connection failed"

Check:
1. Network connectivity: `ping 192.168.20.203`
2. SQL Server is running
3. Credentials in `config.yaml` are correct
4. Firewall allows port 1433

### Queries are slow

1. **Cache the view**: `python scripts/cache_view.py <view_name>`
2. **Use metrics** instead of detail views
3. **Add WHERE clauses** to limit data scanned
4. **Create indexes** on SQL Server for frequently filtered columns

## Architecture Notes

```
┌─────────────────────────────────────┐
│   Your Python App / Jupyter / CLI  │
│   (pandas, matplotlib, etc.)        │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│        DuckDB Semantic Layer        │
│   • Business views (clean names)    │
│   • Metrics (pre-aggregated)        │
│   • Optional cache (materialized)   │
└──────────────┬──────────────────────┘
               │ Federated Queries
               │ (via ODBC)
               ▼
┌─────────────────────────────────────┐
│      SQL Server (192.168.20.203)    │
│      hrmsdb database                │
│   • No data leaves SQL Server       │
│   • Queries are pushed down         │
└─────────────────────────────────────┘
```

**Key Benefits:**
- 🚀 **No ETL pipeline** - Views defined in SQL only
- 💾 **No data duplication** - Query live data
- ⚡ **Fast analytics** - DuckDB's query optimizer
- 🔧 **Easy to maintain** - Just SQL files
- 📊 **Business-friendly** - Clean column names

## Next Steps

1. Explore the example queries: `python scripts/query_example.py`
2. Connect from Jupyter notebooks for analysis
3. Create custom views for your use cases
4. Set up scheduled cache refreshes for dashboards
5. Add data quality checks in `scripts/data_quality_checks.py`

## Support

For issues or questions, refer to:
- DuckDB documentation: https://duckdb.org/docs/
- SQL Server ODBC: https://learn.microsoft.com/en-us/sql/connect/odbc/
