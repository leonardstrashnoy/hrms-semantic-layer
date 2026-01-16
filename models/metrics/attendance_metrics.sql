-- Metrics view: Attendance Metrics
-- Aggregated attendance metrics by week and shift

CREATE OR REPLACE VIEW metrics.attendance_metrics AS

SELECT
    week_number,
    shift,
    dept_id,

    COUNT(DISTINCT employee_id) AS employee_count,
    COUNT(*) AS total_records,

    SUM(total_hours) AS total_hours,
    SUM(hours) AS hours_worked,
    ROUND(AVG(total_hours), 2) AS avg_hours,
    ROUND(AVG(rate), 2) AS avg_rate,

    SUM(CASE WHEN shift_diff_flag THEN 1 ELSE 0 END) AS shift_diff_count,
    SUM(CASE WHEN shift_diff_flag THEN hours ELSE 0 END) AS shift_diff_hours

FROM staging.stg_attendance
WHERE employee_id IS NOT NULL
GROUP BY week_number, shift, dept_id
ORDER BY week_number, shift, dept_id;
