-- Metrics view: Executive Alerts
-- Purpose: Highlight issues requiring attention

CREATE OR REPLACE VIEW metrics.executive_alerts AS

WITH high_arrears AS (
    -- High Arrears Alert (> $500)
    SELECT
        'HIGH_ARREARS' AS alert_type,
        'High' AS priority,
        employee_id,
        TRIM(first_name) || ' ' || TRIM(last_name) AS employee_name,
        'Employee has high arrears balance' AS description,
        CAST(current_arrears AS VARCHAR) AS metric_value,
        'Amount: $' || CAST(ROUND(current_arrears, 2) AS VARCHAR) AS detail,
        _loaded_at AS detected_at
    FROM staging.stg_payroll
    WHERE current_arrears > 500
),

chronic_lateness AS (
    -- Chronic Lateness (> 5 late arrivals in 30 days)
    SELECT
        'CHRONIC_LATENESS' AS alert_type,
        'Medium' AS priority,
        CAST(a.employee_int_id AS VARCHAR) AS employee_id,
        w.full_name AS employee_name,
        'Employee frequently late in last 30 days' AS description,
        CAST(COUNT(*) AS VARCHAR) AS metric_value,
        'Late ' || CAST(COUNT(*) AS VARCHAR) || ' times in 30 days' AS detail,
        MAX(a._loaded_at) AS detected_at
    FROM staging.stg_daily_attendance a
    LEFT JOIN staging.stg_workforce w ON a.employee_int_id = w.employee_int_id
    WHERE a.attendance_date >= CURRENT_DATE - INTERVAL '30 days'
      AND a.is_late = TRUE
    GROUP BY a.employee_int_id, w.full_name
    HAVING COUNT(*) > 5
),

dept_absence AS (
    -- Department High Absence Rate (> 10%)
    SELECT
        'DEPT_HIGH_ABSENCE' AS alert_type,
        'Medium' AS priority,
        w.department_name AS employee_id,
        w.department_name AS employee_name,
        'Department has high absence rate' AS description,
        CAST(ROUND(100.0 * SUM(CASE WHEN a.is_absent THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS VARCHAR) AS metric_value,
        CAST(ROUND(100.0 * SUM(CASE WHEN a.is_absent THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS VARCHAR) || '% absence rate' AS detail,
        MAX(a._loaded_at) AS detected_at
    FROM staging.stg_daily_attendance a
    LEFT JOIN staging.stg_workforce w ON a.employee_int_id = w.employee_int_id
    WHERE a.attendance_date >= CURRENT_DATE - INTERVAL '30 days'
      AND w.department_name IS NOT NULL
    GROUP BY w.department_name
    HAVING (100.0 * SUM(CASE WHEN a.is_absent THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) > 10
),

excessive_ot AS (
    -- Excessive OT (> 20 hours in latest period)
    SELECT
        'EXCESSIVE_OT' AS alert_type,
        'Low' AS priority,
        CAST(a.employee_int_id AS VARCHAR) AS employee_id,
        w.full_name AS employee_name,
        'Employee has excessive overtime' AS description,
        CAST(ROUND(SUM(a.ot_duration) / 60.0, 1) AS VARCHAR) AS metric_value,
        CAST(ROUND(SUM(a.ot_duration) / 60.0, 1) AS VARCHAR) || ' OT hours in 30 days' AS detail,
        MAX(a._loaded_at) AS detected_at
    FROM staging.stg_daily_attendance a
    LEFT JOIN staging.stg_workforce w ON a.employee_int_id = w.employee_int_id
    WHERE a.attendance_date >= CURRENT_DATE - INTERVAL '30 days'
      AND a.ot_duration > 0
    GROUP BY a.employee_int_id, w.full_name
    HAVING SUM(a.ot_duration) / 60.0 > 20
),

recent_terminations AS (
    -- New Terminations (last 7 days)
    SELECT
        'RECENT_TERMINATION' AS alert_type,
        'Low' AS priority,
        employee_id,
        full_name AS employee_name,
        'Employee recently terminated' AS description,
        CAST(status_date AS VARCHAR) AS metric_value,
        'Terminated on ' || CAST(status_date AS VARCHAR) AS detail,
        _loaded_at AS detected_at
    FROM staging.stg_workforce
    WHERE employee_status = 'Terminated'
      AND status_date >= CURRENT_DATE - INTERVAL '7 days'
),

all_alerts AS (
    -- Combine all alerts
    SELECT * FROM high_arrears
    UNION ALL
    SELECT * FROM chronic_lateness
    UNION ALL
    SELECT * FROM dept_absence
    UNION ALL
    SELECT * FROM excessive_ot
    UNION ALL
    SELECT * FROM recent_terminations
)

SELECT
    alert_type,
    priority,
    employee_id,
    employee_name,
    description,
    metric_value,
    detail,
    detected_at
FROM all_alerts
ORDER BY
    CASE priority
        WHEN 'High' THEN 1
        WHEN 'Medium' THEN 2
        WHEN 'Low' THEN 3
    END,
    alert_type,
    employee_name;
