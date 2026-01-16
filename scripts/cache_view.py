#!/usr/bin/env python3
"""
Materialize a specific view locally for faster access.
Usage: python scripts/cache_view.py <view_name>
Example: python scripts/cache_view.py business.employee_summary
"""

import duckdb
import yaml
import sys
from datetime import datetime


def cache_view(view_name, duckdb_path="hrmsdb.duckdb"):
    """Materialize a view into a local table."""
    print(f"Caching view: {view_name}")
    print("=" * 60)

    conn = duckdb.connect(duckdb_path)

    try:
        # Parse schema and view name
        if '.' in view_name:
            schema, table = view_name.split('.')
        else:
            schema = 'business'
            table = view_name

        cache_table = f"cache.{table}"

        print(f"Source: {schema}.{table}")
        print(f"Target: {cache_table}")

        # Check if view exists
        check = conn.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table}'
        """).fetchone()[0]

        if check == 0:
            print(f"✗ Error: View {schema}.{table} does not exist")
            return

        # Drop existing cache table
        conn.execute(f"DROP TABLE IF EXISTS {cache_table}")

        # Create cache table
        print("\nMaterializing view...")
        start_time = datetime.now()

        conn.execute(f"""
            CREATE TABLE {cache_table} AS
            SELECT * FROM {schema}.{table}
        """)

        elapsed = (datetime.now() - start_time).total_seconds()

        # Get row count
        row_count = conn.execute(f"SELECT COUNT(*) FROM {cache_table}").fetchone()[0]

        # Update metadata
        conn.execute("""
            INSERT OR REPLACE INTO _metadata.materialized_views
            VALUES (?, ?, ?, ?)
        """, [view_name, datetime.now(), row_count, 'success'])

        print(f"✓ Cached {row_count:,} rows in {elapsed:.2f} seconds")
        print(f"\nQuery the cached data using: SELECT * FROM {cache_table}")

    except Exception as e:
        print(f"✗ Error: {e}")

        # Update metadata with error
        conn.execute("""
            INSERT OR REPLACE INTO _metadata.materialized_views
            VALUES (?, ?, ?, ?)
        """, [view_name, datetime.now(), 0, f'failed: {str(e)}'])

    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/cache_view.py <view_name>")
        print("\nExamples:")
        print("  python scripts/cache_view.py business.employee_summary")
        print("  python scripts/cache_view.py metrics.monthly_payroll_metrics")
        sys.exit(1)

    view_name = sys.argv[1]
    cache_view(view_name)
