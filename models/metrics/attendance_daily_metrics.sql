-- Metrics view: Attendance Daily Metrics
-- Purpose: Daily attendance rates by department

CREATE OR REPLACE VIEW metrics.attendance_daily_metrics AS

WITH daily_dept_attendance AS (
    SELECT
        a.attendance_date,
        w.department_name,
        w.department_number,

        -- Record counts
        COUNT(DISTINCT a.attendance_id) AS total_records,
        COUNT(DISTINCT a.employee_int_id) AS unique_employees,

        -- Attendance status counts
        COUNT(DISTINCT CASE WHEN a.attendance_status_id = 1 THEN a.attendance_id END) AS present_count,
        COUNT(DISTINCT CASE WHEN a.attendance_status_id = 2 THEN a.attendance_id END) AS absent_count,
        COUNT(DISTINCT CASE WHEN a.attendance_status_id = 3 THEN a.attendance_id END) AS leave_count,
        COUNT(DISTINCT CASE WHEN a.attendance_status_id = 4 THEN a.attendance_id END) AS holiday_count,
        COUNT(DISTINCT CASE WHEN a.attendance_status_id = 5 THEN a.attendance_id END) AS half_day_count,

        -- Late/Early counts
        COUNT(DISTINCT CASE WHEN a.is_late THEN a.attendance_id END) AS late_count,
        COUNT(DISTINCT CASE WHEN a.is_early_leave THEN a.attendance_id END) AS early_leave_count,
        SUM(a.late_by_mins) AS total_late_minutes,

        -- Hours
        SUM(a.total_duration) / 60.0 AS total_hours,
        SUM(a.pay_duration) / 60.0 AS pay_hours,
        SUM(a.ot_duration) / 60.0 AS ot_hours,
        SUM(a.total_break_mins) / 60.0 AS break_hours,

        -- On-call
        SUM(a.on_call_duration) / 60.0 AS on_call_hours

    FROM staging.stg_daily_attendance a
    LEFT JOIN staging.stg_workforce w ON a.employee_int_id = w.employee_int_id
    WHERE a.attendance_date IS NOT NULL
    GROUP BY a.attendance_date, w.department_name, w.department_number
)

SELECT
    attendance_date,
    EXTRACT(YEAR FROM attendance_date) AS attendance_year,
    EXTRACT(MONTH FROM attendance_date) AS attendance_month,
    EXTRACT(WEEK FROM attendance_date) AS attendance_week,
    EXTRACT(DOW FROM attendance_date) AS day_of_week,
    CASE EXTRACT(DOW FROM attendance_date)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,

    department_name,
    department_number,

    -- Counts
    total_records,
    unique_employees,
    present_count,
    absent_count,
    leave_count,
    holiday_count,
    half_day_count,
    late_count,
    early_leave_count,

    -- Rates (percentages)
    CASE
        WHEN total_records > 0
        THEN ROUND(100.0 * present_count / total_records, 1)
        ELSE 0
    END AS presence_rate,

    CASE
        WHEN total_records > 0
        THEN ROUND(100.0 * absent_count / total_records, 1)
        ELSE 0
    END AS absence_rate,

    CASE
        WHEN total_records > 0
        THEN ROUND(100.0 * late_count / total_records, 1)
        ELSE 0
    END AS late_rate,

    -- Hours
    ROUND(total_hours, 2) AS total_hours,
    ROUND(pay_hours, 2) AS pay_hours,
    ROUND(ot_hours, 2) AS ot_hours,
    ROUND(break_hours, 2) AS break_hours,
    ROUND(on_call_hours, 2) AS on_call_hours,

    -- Average hours per employee
    CASE
        WHEN unique_employees > 0
        THEN ROUND(total_hours / unique_employees, 2)
        ELSE 0
    END AS avg_hours_per_employee,

    -- Late minutes
    ROUND(total_late_minutes, 0) AS total_late_minutes,
    CASE
        WHEN late_count > 0
        THEN ROUND(total_late_minutes / late_count, 0)
        ELSE 0
    END AS avg_late_minutes,

    CURRENT_TIMESTAMP AS calculated_at

FROM daily_dept_attendance
ORDER BY attendance_date DESC, department_name;
