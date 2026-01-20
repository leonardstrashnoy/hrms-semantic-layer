#!/usr/bin/env python3
"""
Test the connection to SQL Server using pymssql.
"""

import os
import pymssql
import yaml


def test_connection():
    """Test SQL Server connection via pymssql."""
    print("=" * 60)
    print("Testing SQL Server Connection")
    print("=" * 60)

    with open("config.yaml", 'r') as f:
        config = yaml.safe_load(f)

    sql_config = config['sql_server']
    username = os.getenv('SQL_SERVER_USERNAME') or sql_config.get('username')
    password = os.getenv('SQL_SERVER_PASSWORD') or sql_config.get('password')

    print("\n1. Checking credentials...")
    if not username or not password:
        print("  ✗ SQL Server credentials not found.")
        print("    Set SQL_SERVER_USERNAME and SQL_SERVER_PASSWORD, or update config.yaml.")
        return
    print("  ✓ Credentials found")

    print("\n2. Connecting to SQL Server (pymssql)...")
    try:
        conn = pymssql.connect(
            server=sql_config['host'],
            port=sql_config['port'],
            user=username,
            password=password,
            database=sql_config['database']
        )
        print("  ✓ Connected successfully")
    except Exception as e:
        print(f"  ✗ Connection failed: {e}")
        return

    print("\n3. Running a simple query...")
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT TOP 5 TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        rows = cursor.fetchall()
        if rows:
            print("  ✓ Sample tables:")
            for row in rows:
                print(f"    - {row[0]}")
        else:
            print("  ⚠️  No tables returned")
    except Exception as e:
        print(f"  ✗ Query failed: {e}")
    finally:
        conn.close()

    print("\n" + "=" * 60)
    print("Connection test complete!")
    print("=" * 60)


if __name__ == "__main__":
    test_connection()
