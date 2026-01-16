#!/usr/bin/env python3
"""
Healthcare-specific analytics queries for hospital HR data.
Examples of common hospital workforce management reports.
"""

import duckdb
import pandas as pd


def run_healthcare_analytics(duckdb_path="hrmsdb.duckdb"):
    """Run healthcare-specific analytics queries."""
    print("=" * 70)
    print("Hospital Healthcare Workforce Analytics")
    print("=" * 70)

    conn = duckdb.connect(duckdb_path, read_only=True)

    # Report 1: Clinical Staff by Role and Unit
    print("\n1. Clinical Staff Distribution by Role and Care Unit")
    print("-" * 70)
    try:
        result = conn.execute("""
            SELECT
                clinical_role,
                care_unit_type,
                COUNT(*) as staff_count,
                COUNT(CASE WHEN employment_status = 'Active' THEN 1 END) as active_count,
                ROUND(AVG(tenure_years), 1) as avg_tenure_years,
                ROUND(AVG(attendance_rate_pct), 1) as avg_attendance_rate
            FROM business.clinical_staff_summary
            GROUP BY clinical_role, care_unit_type
            ORDER BY staff_count DESC
            LIMIT 15
        """).fetchdf()

        print(result.to_string(index=False))
        print(f"\n{len(result)} role-unit combinations")
    except Exception as e:
        print(f"Error: {e}")

    # Report 2: Burnout Risk Analysis
    print("\n\n2. Burnout Risk Analysis (High Overtime Staff)")
    print("-" * 70)
    try:
        result = conn.execute("""
            SELECT
                clinical_role,
                department,
                COUNT(*) as staff_count,
                ROUND(AVG(ytd_overtime_hours), 1) as avg_overtime_hours,
                ROUND(AVG(overtime_percentage), 1) as avg_overtime_pct,
                COUNT(CASE WHEN burnout_risk_level = 'High Risk' THEN 1 END) as high_risk_count,
                COUNT(CASE WHEN burnout_risk_level = 'Moderate Risk' THEN 1 END) as moderate_risk_count
            FROM business.clinical_staff_summary
            WHERE employment_status = 'Active'
            GROUP BY clinical_role, department
            HAVING AVG(ytd_overtime_hours) > 50
            ORDER BY avg_overtime_hours DESC
            LIMIT 15
        """).fetchdf()

        if len(result) > 0:
            print(result.to_string(index=False))
            print(f"\n{len(result)} roles with elevated overtime")
        else:
            print("No high-overtime groups found (good news!)")
    except Exception as e:
        print(f"Error: {e}")

    # Report 3: Shift Coverage Analysis
    print("\n\n3. Shift Coverage - Last 4 Weeks")
    print("-" * 70)
    try:
        result = conn.execute("""
            SELECT
                TO_CHAR(week_start_date, 'YYYY-MM-DD') as week,
                shift_type,
                SUM(staff_count) as total_staff,
                ROUND(AVG(avg_hours_per_shift), 1) as avg_hours_per_shift,
                SUM(total_overtime_hours) as total_overtime_hours,
                ROUND(AVG(overtime_pct), 1) as avg_overtime_pct,
                SUM(extended_shifts_count) as extended_shifts
            FROM metrics.shift_coverage_metrics
            WHERE week_start_date >= CURRENT_DATE - INTERVAL '4 weeks'
            GROUP BY week_start_date, shift_type
            ORDER BY week_start_date DESC, shift_type
        """).fetchdf()

        print(result.to_string(index=False))
        print(f"\n{len(result)} shift-week combinations")
    except Exception as e:
        print(f"Error: {e}")

    # Report 4: Department Staffing Levels
    print("\n\n4. Department Staffing Levels (Current Month)")
    print("-" * 70)
    try:
        result = conn.execute("""
            SELECT
                department,
                shift_type,
                ROUND(avg_rn_count, 1) as avg_rns,
                ROUND(avg_lpn_count, 1) as avg_lpns,
                ROUND(avg_cna_count, 1) as avg_cnas,
                ROUND(avg_total_staff_count, 1) as avg_total_staff,
                min_total_staff_count as min_staff,
                max_total_staff_count as max_staff,
                understaffed_days_pct as pct_understaffed_days
            FROM metrics.department_staffing_ratios
            WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
              AND month = EXTRACT(MONTH FROM CURRENT_DATE)
            ORDER BY department, shift_type
        """).fetchdf()

        print(result.to_string(index=False))
        print(f"\n{len(result)} department-shift combinations")
    except Exception as e:
        print(f"Error: {e}")

    # Report 5: Critical Care Units Focus
    print("\n\n5. Critical Care Units Workforce Summary")
    print("-" * 70)
    try:
        result = conn.execute("""
            SELECT
                care_unit_type,
                clinical_role,
                staff_count,
                active_count,
                ROUND(avg_tenure_years, 1) as avg_tenure,
                ROUND(avg_attendance_rate, 1) as attendance_rate,
                high_burnout_risk_count,
                ROUND(avg_overtime_pct, 1) as avg_overtime_pct
            FROM metrics.clinical_workforce_metrics
            WHERE care_unit_type IN ('ICU/Critical Care', 'Emergency Department', 'Surgical Services')
              AND employment_status = 'Active'
            ORDER BY care_unit_type, staff_count DESC
        """).fetchdf()

        print(result.to_string(index=False))
        print(f"\n{len(result)} critical care staffing groups")
    except Exception as e:
        print(f"Error: {e}")

    # Report 6: Weekend and Night Shift Differential Analysis
    print("\n\n6. Shift Differential Pay Analysis (Recent Month)")
    print("-" * 70)
    try:
        result = conn.execute("""
            SELECT
                shift_type,
                day_type,
                SUM(staff_count) as total_staff,
                SUM(total_hours_worked) as total_hours,
                SUM(shifts_with_differential) as differential_shifts,
                ROUND(AVG(differential_pct), 1) as avg_differential_pct
            FROM metrics.shift_coverage_metrics
            WHERE year_month = TO_CHAR(CURRENT_DATE, 'YYYY-MM')
            GROUP BY shift_type, day_type
            ORDER BY shift_type, day_type
        """).fetchdf()

        print(result.to_string(index=False))
    except Exception as e:
        print(f"Error: {e}")

    # Report 7: Turnover Risk Indicators
    print("\n\n7. Potential Turnover Risk - New vs Experienced Staff")
    print("-" * 70)
    try:
        result = conn.execute("""
            SELECT
                clinical_role,
                care_unit_type,
                staff_count,
                new_hires_under_1_year,
                experienced_staff_5plus_years,
                ROUND(new_hires_under_1_year::DECIMAL / NULLIF(staff_count, 0) * 100, 1) as new_hire_pct,
                ROUND(experienced_staff_5plus_years::DECIMAL / NULLIF(staff_count, 0) * 100, 1) as experienced_pct
            FROM metrics.clinical_workforce_metrics
            WHERE employment_status = 'Active'
              AND staff_count >= 5
            ORDER BY new_hire_pct DESC
            LIMIT 15
        """).fetchdf()

        print(result.to_string(index=False))
        print(f"\n{len(result)} units analyzed")
        print("\nHigh new hire % may indicate retention issues or rapid growth")
    except Exception as e:
        print(f"Error: {e}")

    conn.close()

    print("\n" + "=" * 70)
    print("Healthcare Analytics Complete!")
    print("=" * 70)
    print("\nKey Insights to Monitor:")
    print("  • Burnout risk levels by department and role")
    print("  • Shift coverage gaps (especially nights and weekends)")
    print("  • Staffing ratios in critical care units")
    print("  • Overtime trends (cost and retention impact)")
    print("  • New hire integration and turnover risk")


if __name__ == "__main__":
    run_healthcare_analytics()
