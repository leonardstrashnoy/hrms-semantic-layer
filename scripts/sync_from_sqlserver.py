#!/usr/bin/env python3
"""
Sync data from SQL Server to DuckDB.
This script extracts data from SQL Server and loads it into DuckDB's raw schema.
"""

import pyodbc
import duckdb
import yaml
import os
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd

load_dotenv()


class SQLServerSyncManager:
    def __init__(self, config_path="config.yaml"):
        """Initialize sync manager."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.sql_config = self.config['sql_server']
        self.duckdb_path = self.config['duckdb']['database_path']
        self.batch_size = self.config['sync'].get('batch_size', 10000)

        self.sql_conn = None
        self.duck_conn = None

    def connect_sql_server(self):
        """Connect to SQL Server."""
        print("Connecting to SQL Server...")
        connection_string = (
            f"DRIVER={{{self.sql_config['driver']}}};"
            f"SERVER={self.sql_config['host']},{self.sql_config['port']};"
            f"DATABASE={self.sql_config['database']};"
            f"UID={self.sql_config['username']};"
            f"PWD={self.sql_config['password']}"
        )
        self.sql_conn = pyodbc.connect(connection_string)
        print("✓ Connected to SQL Server")

    def connect_duckdb(self):
        """Connect to DuckDB."""
        print(f"Connecting to DuckDB at {self.duckdb_path}...")
        self.duck_conn = duckdb.connect(self.duckdb_path)
        print("✓ Connected to DuckDB")

    def get_table_list(self):
        """Get list of tables to sync from SQL Server."""
        if 'tables' in self.config['sync']:
            return self.config['sync']['tables']
        else:
            # Query SQL Server for all user tables
            query = """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_SCHEMA = 'dbo'
            """
            cursor = self.sql_conn.cursor()
            cursor.execute(query)
            return [row[0] for row in cursor.fetchall()]

    def sync_table(self, table_name):
        """Sync a single table from SQL Server to DuckDB."""
        print(f"\n--- Syncing table: {table_name} ---")

        try:
            # Get row count from SQL Server
            count_query = f'SELECT COUNT(*) FROM [{table_name}]'
            cursor = self.sql_conn.cursor()
            cursor.execute(count_query)
            row_count = cursor.fetchone()[0]
            print(f"  Rows in SQL Server: {row_count:,}")

            if row_count == 0:
                print(f"  ⚠️  Table is empty, skipping")
                return

            # Extract data from SQL Server
            print(f"  Extracting data...")
            query = f'SELECT * FROM [{table_name}]'
            df = pd.read_sql(query, self.sql_conn)

            # Clean column names (remove special characters, spaces)
            df.columns = [col.replace(' ', '_').replace('$', '').replace('#', '') for col in df.columns]

            print(f"  Extracted {len(df):,} rows")

            # Load into DuckDB raw schema
            print(f"  Loading into DuckDB...")

            # Drop existing table
            self.duck_conn.execute(f'DROP TABLE IF EXISTS raw."{table_name}"')

            # Create table and insert data
            self.duck_conn.execute(f'CREATE TABLE raw."{table_name}" AS SELECT * FROM df')

            # Verify load
            verify_count = self.duck_conn.execute(
                f'SELECT COUNT(*) FROM raw."{table_name}"'
            ).fetchone()[0]
            print(f"  ✓ Loaded {verify_count:,} rows into DuckDB")

            # Update metadata
            self.update_metadata(table_name, row_count, 'success')

        except Exception as e:
            print(f"  ✗ Error syncing {table_name}: {str(e)}")
            self.update_metadata(table_name, 0, f'failed: {str(e)}')

    def update_metadata(self, table_name, row_count, status):
        """Update sync metadata in DuckDB."""
        self.duck_conn.execute("""
            INSERT OR REPLACE INTO _metadata.data_freshness
            VALUES (?, ?, ?, ?)
        """, [table_name, datetime.now(), row_count, status])

    def sync_all(self):
        """Sync all configured tables."""
        print("=" * 60)
        print("SQL Server to DuckDB Sync")
        print("=" * 60)

        self.connect_sql_server()
        self.connect_duckdb()

        # Ensure raw schema exists
        self.duck_conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

        tables = self.get_table_list()
        print(f"\nFound {len(tables)} tables to sync")

        success_count = 0
        for i, table in enumerate(tables, 1):
            print(f"\n[{i}/{len(tables)}] ", end="")
            try:
                self.sync_table(table)
                success_count += 1
            except Exception as e:
                print(f"Error syncing {table}: {e}")

        print("\n" + "=" * 60)
        print(f"✓ Sync complete: {success_count}/{len(tables)} tables synced successfully")
        print("=" * 60)

    def close(self):
        """Close all connections."""
        if self.sql_conn:
            self.sql_conn.close()
        if self.duck_conn:
            self.duck_conn.close()


if __name__ == "__main__":
    import sys

    sync_manager = SQLServerSyncManager()
    try:
        if len(sys.argv) > 1:
            # Sync specific table
            table_name = sys.argv[1]
            sync_manager.connect_sql_server()
            sync_manager.connect_duckdb()
            sync_manager.sync_table(table_name)
        else:
            # Sync all tables
            sync_manager.sync_all()
    finally:
        sync_manager.close()
