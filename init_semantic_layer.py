#!/usr/bin/env python3
"""
Initialize the DuckDB semantic layer for HRMS database.
This script creates the DuckDB database and imports data from SQL Server
using pymssql (works on ARM Mac without ODBC).
"""

import duckdb
import yaml
import os
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import pandas as pd

# Load environment variables (interpolate=False to preserve $ in passwords)
load_dotenv(interpolate=False, override=True)


class SemanticLayerInitializer:
    def __init__(self, config_path="config.yaml"):
        """Initialize the semantic layer."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.duckdb_path = self.config['duckdb']['database_path']
        self.conn = None

    def connect(self):
        """Connect to DuckDB and configure."""
        print(f"Connecting to DuckDB at {self.duckdb_path}...")
        self.conn = duckdb.connect(self.duckdb_path)

        # Install and load extensions
        for ext in ['fts', 'excel', 'vss', 'httpfs']:
            self.conn.execute(f"INSTALL {ext}; LOAD {ext};")

        # Configure DuckDB
        memory_limit = self.config['duckdb'].get('memory_limit', '4GB')
        threads = self.config['duckdb'].get('threads', 4)

        self.conn.execute(f"SET memory_limit='{memory_limit}'")
        self.conn.execute(f"SET threads={threads}")

        print("✓ Connected to DuckDB")

    def connect_sql_server(self):
        """Connect to SQL Server using SQLAlchemy with pymssql driver.

        Credentials are read from environment variables first,
        falling back to config.yaml values.
        """
        sql_config = self.config['sql_server']

        # Get credentials from env vars (preferred) or config (fallback)
        username = os.getenv('SQL_SERVER_USERNAME') or sql_config.get('username')
        password = os.getenv('SQL_SERVER_PASSWORD') or sql_config.get('password')

        if not username or not password:
            raise ValueError(
                "SQL Server credentials not found. Set SQL_SERVER_USERNAME and "
                "SQL_SERVER_PASSWORD environment variables, or add them to config.yaml"
            )

        # URL-encode credentials to handle special characters
        encoded_password = quote_plus(password)
        connection_string = (
            f"mssql+pymssql://{username}:{encoded_password}@"
            f"{sql_config['host']}:{sql_config['port']}/{sql_config['database']}"
        )
        return create_engine(connection_string)

    def setup_sql_server_attachment(self):
        """Test SQL Server connection using SQLAlchemy."""
        print("\nSetting up SQL Server connection...")

        sql_config = self.config['sql_server']

        try:
            # Test connection with SQLAlchemy
            print(f"  Connecting to SQL Server at {sql_config['host']}...")
            engine = self.connect_sql_server()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            print("  ✓ SQL Server connection successful (using SQLAlchemy + pymssql)")

            # Store connection info for metadata
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS _metadata.sql_server_connection (
                    host VARCHAR,
                    database_name VARCHAR,
                    connected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            self.conn.execute("""
                DELETE FROM _metadata.sql_server_connection
            """)

            self.conn.execute("""
                INSERT INTO _metadata.sql_server_connection (host, database_name)
                VALUES (?, ?)
            """, [sql_config['host'], sql_config['database']])

            print("  ✓ Connection info stored")
            print("\n  NOTE: Data will be imported from SQL Server into DuckDB tables")
            print("  Using SQLAlchemy + pymssql (ARM Mac compatible)")

        except Exception as e:
            print(f"  ⚠️  Warning: Could not connect to SQL Server: {e}")
            print("  Check your config.yaml settings")
            raise

    def create_schemas(self):
        """Create schema structure for semantic layer."""
        print("\nCreating schema structure...")

        schemas = [
            '_metadata',  # Internal metadata
            'raw',        # Raw views pointing to SQL Server
            'staging',    # Cleaned and typed data
            'business',   # Business-friendly views
            'metrics',    # Aggregated metrics
            'cache'       # Materialized views (optional)
        ]

        for schema in schemas:
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            print(f"  ✓ Schema: {schema}")

    def create_raw_view_helpers(self):
        """Create helper functions for data import."""
        print("\nCreating raw view helpers...")

        # Create import tracking table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS _metadata.import_log (
                table_name VARCHAR,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_count BIGINT,
                status VARCHAR
            )
        """)

        print("  ✓ Import tracking table created")

    def sanitize_duckdb_name(self, name: str) -> str:
        """Convert SQL table name to valid DuckDB table name."""
        # Remove quotes if present
        name = name.strip("'\"")
        # Replace special characters
        sanitized = name.replace(' ', '_').replace('$', '').replace('-', '_').replace("'", '').lower()
        # If starts with a number, prefix with underscore
        if sanitized and sanitized[0].isdigit():
            sanitized = f"t_{sanitized}"
        return sanitized

    def import_table_from_sql_server(self, sql_table_name: str, duckdb_table_name: str,
                                      date_column: str = None, days_back: int = None):
        """Import a table from SQL Server into DuckDB.

        Args:
            sql_table_name: Name of table in SQL Server
            duckdb_table_name: Target table name in DuckDB
            date_column: Optional column name for date filtering
            days_back: Optional number of days to look back (filters data)
        """
        try:
            engine = self.connect_sql_server()

            # Handle table names with special characters (including embedded quotes)
            clean_name = sql_table_name.strip("'\"")
            escaped_name = f"[{clean_name}]"

            # Build query with optional date filter
            if date_column and days_back:
                query = f"""
                    SELECT * FROM {escaped_name}
                    WHERE [{date_column}] >= DATEADD(day, -{days_back}, GETDATE())
                """
            else:
                query = f"SELECT * FROM {escaped_name}"

            # Read data into pandas DataFrame
            df = pd.read_sql(query, engine)
            engine.dispose()

            # Insert into DuckDB - split schema and table name for proper creation
            if '.' in duckdb_table_name:
                schema, table = duckdb_table_name.split('.', 1)
                self.conn.execute(f'DROP TABLE IF EXISTS {schema}."{table}"')
                self.conn.execute(f'CREATE TABLE {schema}."{table}" AS SELECT * FROM df')
            else:
                self.conn.execute(f'DROP TABLE IF EXISTS "{duckdb_table_name}"')
                self.conn.execute(f'CREATE TABLE "{duckdb_table_name}" AS SELECT * FROM df')

            # Log the import
            row_count = len(df)
            self.conn.execute("""
                INSERT INTO _metadata.import_log (table_name, row_count, status)
                VALUES (?, ?, 'success')
            """, [duckdb_table_name, row_count])

            return row_count
        except Exception as e:
            self.conn.execute("""
                INSERT INTO _metadata.import_log (table_name, row_count, status)
                VALUES (?, 0, ?)
            """, [duckdb_table_name, f'error: {str(e)}'])
            raise

    def create_example_raw_views(self):
        """Import tables from SQL Server into DuckDB raw schema."""
        print("\nImporting tables from SQL Server...")

        # Get tables to sync from config
        sync_config = self.config.get('sync', {})
        tables = sync_config.get('tables', [])
        activity_log_days = sync_config.get('activity_log_days', 30)

        if not tables:
            # Default example tables if none configured
            tables = [
                "Activity_Log",
                "CRMC_PayrollFile",
            ]

        for sql_table in tables:
            # Convert SQL table name to valid DuckDB table name
            duckdb_name = self.sanitize_duckdb_name(sql_table)
            duckdb_table = f"raw.{duckdb_name}"

            # Check if this is an activity_log table (filter to last N days)
            is_activity_log = 'activity_log' in sql_table.lower()

            try:
                if is_activity_log:
                    print(f"  Importing: {sql_table} -> {duckdb_table} (last {activity_log_days} days)...")
                    row_count = self.import_table_from_sql_server(
                        sql_table, duckdb_table,
                        date_column='EnteredDate',
                        days_back=activity_log_days
                    )
                else:
                    print(f"  Importing: {sql_table} -> {duckdb_table}...")
                    row_count = self.import_table_from_sql_server(sql_table, duckdb_table)
                print(f"  ✓ Imported: {duckdb_table} ({row_count:,} rows)")
            except Exception as e:
                print(f"  ⚠️  Could not import {sql_table}: {e}")

    def create_staging_views(self):
        """Create staging views with cleaned data."""
        print("\nCreating staging views...")

        # Read SQL files from models/staging/
        staging_path = Path("models/staging")
        if staging_path.exists():
            for sql_file in sorted(staging_path.glob("*.sql")):
                try:
                    print(f"  Creating: {sql_file.stem}...")
                    with open(sql_file, 'r') as f:
                        sql_content = f.read()
                        # Update queries to use raw schema
                        self.conn.execute(sql_content)
                    print(f"  ✓ {sql_file.stem}")
                except Exception as e:
                    print(f"  ⚠️  Error with {sql_file.stem}: {e}")
        else:
            print("  ⚠️  No staging models found")

    def create_business_views(self):
        """Create business-friendly views."""
        print("\nCreating business views...")

        business_path = Path("models/business")
        if business_path.exists():
            for sql_file in sorted(business_path.glob("*.sql")):
                try:
                    print(f"  Creating: {sql_file.stem}...")
                    with open(sql_file, 'r') as f:
                        self.conn.execute(f.read())
                    print(f"  ✓ {sql_file.stem}")
                except Exception as e:
                    print(f"  ⚠️  Error with {sql_file.stem}: {e}")
        else:
            print("  ⚠️  No business models found")

    def create_metrics(self):
        """Create aggregated metrics views."""
        print("\nCreating metrics...")

        metrics_path = Path("models/metrics")
        if metrics_path.exists():
            for sql_file in sorted(metrics_path.glob("*.sql")):
                try:
                    print(f"  Creating: {sql_file.stem}...")
                    with open(sql_file, 'r') as f:
                        self.conn.execute(f.read())
                    print(f"  ✓ {sql_file.stem}")
                except Exception as e:
                    print(f"  ⚠️  Error with {sql_file.stem}: {e}")
        else:
            print("  ⚠️  No metrics models found")

    def create_metadata_tables(self):
        """Create metadata tracking tables."""
        print("\nCreating metadata tables...")

        # Track materialized views
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS _metadata.materialized_views (
                view_name VARCHAR,
                last_refresh TIMESTAMP,
                row_count BIGINT,
                refresh_status VARCHAR
            )
        """)

        # Track data quality metrics
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS _metadata.data_quality (
                check_name VARCHAR,
                table_name VARCHAR,
                check_time TIMESTAMP,
                passed BOOLEAN,
                details VARCHAR
            )
        """)

        print("  ✓ Metadata tables created")

    def run(self):
        """Run the full initialization."""
        print("=" * 60)
        print("HRMS Semantic Layer Initialization")
        print("Data Import Mode (using pymssql)")
        print("=" * 60)

        self.connect()
        self.create_schemas()
        self.create_metadata_tables()
        self.setup_sql_server_attachment()
        self.create_raw_view_helpers()
        self.create_example_raw_views()
        self.create_staging_views()
        self.create_business_views()
        self.create_metrics()

        print("\n" + "=" * 60)
        print("✓ Semantic layer initialized successfully!")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Test queries: python scripts/query_example.py")
        print("2. Re-run this script to refresh data from SQL Server")
        print("\nData has been imported into DuckDB tables.")
        print("Queries will be fast (local DuckDB) - no SQL Server latency.")

        self.conn.close()


if __name__ == "__main__":
    initializer = SemanticLayerInitializer()
    initializer.run()
