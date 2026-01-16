#!/usr/bin/env python3
"""
Test the connection to SQL Server through DuckDB.
"""

import duckdb
import yaml


def test_connection():
    """Test SQL Server connection via DuckDB ODBC."""
    print("=" * 60)
    print("Testing SQL Server Connection")
    print("=" * 60)

    with open("config.yaml", 'r') as f:
        config = yaml.safe_load(f)

    conn = duckdb.connect(config['duckdb']['database_path'])

    # Test 1: Check if ODBC extension is loaded
    print("\n1. Checking ODBC extension...")
    try:
        extensions = conn.execute("SELECT * FROM duckdb_extensions()").fetchdf()
        odbc_loaded = extensions[extensions['extension_name'] == 'odbc']['loaded'].values
        if len(odbc_loaded) > 0 and odbc_loaded[0]:
            print("  ✓ ODBC extension is loaded")
        else:
            print("  ⚠️  ODBC extension not loaded")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Test 2: Check connection string
    print("\n2. Checking stored connection...")
    try:
        result = conn.execute("""
            SELECT host, database, attached_at
            FROM _metadata.sql_server_connection
            LIMIT 1
        """).fetchone()

        if result:
            print(f"  ✓ Host: {result[0]}")
            print(f"  ✓ Database: {result[1]}")
            print(f"  ✓ Configured at: {result[2]}")
        else:
            print("  ⚠️  No connection configured")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Test 3: Try to query a raw view
    print("\n3. Testing raw view query...")
    try:
        # Try to query a raw view (this will hit SQL Server)
        result = conn.execute("""
            SELECT COUNT(*) as view_count
            FROM information_schema.views
            WHERE table_schema = 'raw'
        """).fetchone()

        if result and result[0] > 0:
            print(f"  ✓ Found {result[0]} raw views")

            # Try to get table names from SQL Server
            print("\n4. Attempting to query SQL Server...")
            try:
                conn_str_result = conn.execute("""
                    SELECT connection_string FROM _metadata.sql_server_connection LIMIT 1
                """).fetchone()

                if conn_str_result:
                    conn_str = conn_str_result[0]
                    print(f"  Using connection string: {conn_str[:50]}...")

                    # Try to list tables from SQL Server
                    tables = conn.execute(f"""
                        SELECT *
                        FROM odbc_scan('{conn_str}',
                                       'INFORMATION_SCHEMA',
                                       'TABLES')
                        WHERE TABLE_TYPE = 'BASE TABLE'
                        LIMIT 5
                    """).fetchdf()

                    print(f"  ✓ Successfully connected to SQL Server!")
                    print(f"  ✓ Sample tables:")
                    print(tables[['TABLE_NAME']].to_string(index=False))

                else:
                    print("  ⚠️  No connection string found")

            except Exception as e:
                print(f"  ⚠️  Could not query SQL Server: {e}")
                print("  This is normal if SQL Server credentials are not configured yet.")

        else:
            print("  ⚠️  No raw views found")
            print("  Run init_semantic_layer.py first")

    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Test 5: Check schemas
    print("\n5. Checking schemas...")
    try:
        schemas = conn.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name IN ('_metadata', 'raw', 'staging', 'business', 'metrics', 'cache')
            ORDER BY schema_name
        """).fetchdf()

        print(f"  ✓ Found {len(schemas)} schemas:")
        for schema in schemas['schema_name']:
            print(f"    - {schema}")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    conn.close()

    print("\n" + "=" * 60)
    print("Connection test complete!")
    print("=" * 60)


if __name__ == "__main__":
    test_connection()
