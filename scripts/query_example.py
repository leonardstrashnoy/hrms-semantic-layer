#!/usr/bin/env python3
"""
Example queries demonstrating the semantic layer.
Run this after initialization to test your setup.
"""

import duckdb
import pandas as pd


def run_examples(duckdb_path="hrmsdb.duckdb"):
    """Run example queries against the semantic layer."""
    print("=" * 60)
    print("Semantic Layer Query Examples")
    print("=" * 60)

    conn = duckdb.connect(duckdb_path, read_only=True)

    # Example 1: Employee Summary
    print("\n1. Active Employees Summary")
    print("-" * 60)
    try:
        result = conn.execute("""
            SELECT
                full_name,
                department,
                job_title,
                tenure_years,
                annual_salary,
                ytd_gross_pay
            FROM business.employee_summary
            WHERE employment_status = 'Active'
            ORDER BY tenure_years DESC
            LIMIT 10
        """).fetchdf()

        print(result.to_string(index=False))
        print(f"\n{len(result)} rows returned")
    except Exception as e:
        print(f"Error: {e}")

    # Example 2: Department Headcount
    print("\n\n2. Headcount by Department")
    print("-" * 60)
    try:
        result = conn.execute("""
            SELECT
                department,
                SUM(active_count) as active_employees,
                ROUND(AVG(avg_annual_salary), 0) as avg_salary,
                SUM(total_annual_salary) as total_payroll
            FROM metrics.headcount_metrics
            WHERE employment_status = 'Active'
            GROUP BY department
            ORDER BY active_employees DESC
        """).fetchdf()

        print(result.to_string(index=False))
        print(f"\n{len(result)} departments")
    except Exception as e:
        print(f"Error: {e}")

    # Example 3: Monthly Payroll Trend
    print("\n\n3. Recent Monthly Payroll")
    print("-" * 60)
    try:
        result = conn.execute("""
            SELECT
                year_month,
                SUM(employee_count) as employees,
                SUM(total_gross_pay) as gross_pay,
                SUM(total_net_pay) as net_pay,
                SUM(total_overtime_hours) as overtime_hours
            FROM metrics.monthly_payroll_metrics
            GROUP BY year_month
            ORDER BY year_month DESC
            LIMIT 12
        """).fetchdf()

        print(result.to_string(index=False))
        print(f"\n{len(result)} months")
    except Exception as e:
        print(f"Error: {e}")

    # Example 4: Attendance Rate
    print("\n\n4. Recent Attendance Rates by Department")
    print("-" * 60)
    try:
        result = conn.execute("""
            SELECT
                year_month,
                department,
                employee_count,
                attendance_rate_pct,
                absence_rate_pct,
                late_rate_pct
            FROM metrics.attendance_metrics
            ORDER BY year_month DESC, department
            LIMIT 10
        """).fetchdf()

        print(result.to_string(index=False))
        print(f"\n{len(result)} rows")
    except Exception as e:
        print(f"Error: {e}")

    # Show available views
    print("\n\n5. Available Views")
    print("-" * 60)
    try:
        result = conn.execute("""
            SELECT
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema IN ('staging', 'business', 'metrics', 'cache')
            ORDER BY table_schema, table_name
        """).fetchdf()

        print(result.to_string(index=False))
        print(f"\n{len(result)} views available")
    except Exception as e:
        print(f"Error: {e}")

    conn.close()

    print("\n" + "=" * 60)
    print("Examples complete!")
    print("=" * 60)
    print("\nNote: If queries are slow, consider caching:")
    print("  python scripts/cache_view.py business.employee_summary")


if __name__ == "__main__":
    run_examples()
