# HRMS Semantic Layer with DuckDB

A semantic layer built on DuckDB that provides a clean, business-friendly interface to the HRMS SQL Server database.

## Overview

This semantic layer:
- Connects to SQL Server database `hrmsdb` at `192.168.20.203:1433` using **pymssql**
- Imports data into DuckDB for fast local queries
- Provides business-friendly views and metrics
- Works on **ARM Mac** (Apple Silicon) without ODBC dependencies
- Activity logs filtered to last 30 days to keep database size manageable

## Architecture

```
SQL Server (hrmsdb)
    ↓ (Data import via pymssql)
DuckDB Semantic Layer
    ├── raw.*           - Imported tables from SQL Server
    ├── staging.*       - Cleaned, typed data views
    ├── business.*      - Denormalized, user-friendly views
    └── metrics.*       - Aggregated KPIs and metrics
```

## Key Features

- **ARM Mac Compatible** - Uses pymssql instead of ODBC
- **Fast Local Queries** - Data stored in DuckDB columnar format
- **Business-Friendly** - Clean column names and structures
- **Configurable Import** - Choose which tables to sync
- **Activity Log Filtering** - Only imports last 30 days

## Setup

### Prerequisites
- Python 3.8+
- Conda (recommended)
- VPN access to SQL Server at 192.168.20.203

### Installation

```bash
# Create conda environment
conda create -n hrms python=3.11 -y
conda activate hrms

# Install dependencies
pip install -r requirements.txt
```

### Configuration

1. Review `config.yaml` for database settings
2. Run initialization: `python init_semantic_layer.py`

## Project Structure

```
hrms-semantic-layer/
├── config.yaml              # Database connection config
├── init_semantic_layer.py   # Initialize DuckDB semantic layer
├── requirements.txt         # Python dependencies
├── models/                  # SQL models for semantic layer
│   ├── staging/            # Cleaned raw data views
│   ├── business/           # Business-friendly views
│   └── metrics/            # Aggregated metrics
└── hrmsdb.duckdb           # DuckDB database file (created on init)
```

## Usage

### Initialize/Refresh Data

```bash
conda activate hrms
python init_semantic_layer.py
```

### Query the Semantic Layer

```python
import duckdb

conn = duckdb.connect('hrmsdb.duckdb')

# Query employee summary
result = conn.execute("""
    SELECT * FROM business.employee_summary
    LIMIT 10
""").fetchdf()
print(result)

# Query benefits metrics
result = conn.execute("""
    SELECT * FROM metrics.headcount_metrics
""").fetchdf()
print(result)
```

### Available Views

#### Staging Views
- `staging.stg_activity_log` - User activity logs (last 30 days)
- `staging.stg_attendance` - Attendance records
- `staging.stg_employees` - Employee information
- `staging.stg_payroll` - Payroll/benefits data

#### Business Views
- `business.employee_summary` - Employee with benefit plan summary
- `business.payroll_detail` - Detailed benefits enrollment
- `business.attendance_detail` - Attendance with employee info
- `business.staffing_by_shift` - Hours by shift and employee
- `business.staff_summary` - Staff benefits overview

#### Metrics Views
- `metrics.headcount_metrics` - Employee counts by benefit type
- `metrics.monthly_payroll_metrics` - Benefits enrollment metrics
- `metrics.attendance_metrics` - Attendance by week/shift
- `metrics.clinical_workforce_metrics` - Workforce summary
- `metrics.department_staffing_ratios` - Staffing by department
- `metrics.shift_coverage_metrics` - Shift coverage analysis

## Data Import Summary

Current import includes:
- **224,433** payroll/benefits records
- **1,129** unique employees
- Attendance records across 4 shifts
- 1099 and 401k data
- Activity logs (last 30 days only)

## Refreshing Data

To refresh data from SQL Server, simply re-run:

```bash
python init_semantic_layer.py
```

This will:
1. Connect to SQL Server via pymssql
2. Import configured tables (see `config.yaml`)
3. Recreate all staging, business, and metrics views

## Dependencies

- `duckdb` - Local analytical database
- `pymssql` - SQL Server connection (ARM Mac compatible)
- `pandas` - Data manipulation
- `pyyaml` - Configuration
- `python-dotenv` - Environment variables
- `sqlalchemy` - SQL toolkit

## Notes

- Activity logs are filtered to last 30 days (configurable in `config.yaml`)
- Tables starting with numbers get `t_` prefix (e.g., `401kdata` → `t_401kdata`)
- Workforce data tables were not available in SQL Server
