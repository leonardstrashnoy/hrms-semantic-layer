-- Metrics view: Shift Coverage Metrics
-- Analyzes staffing levels by shift

CREATE OR REPLACE VIEW metrics.shift_coverage_metrics AS

SELECT
    shift,
    week_number,

    COUNT(DISTINCT employee_id) AS staff_count,
    SUM(record_count) AS total_records,

    SUM(total_hours) AS total_hours,
    SUM(hours) AS hours_worked,
    ROUND(AVG(avg_rate), 2) AS avg_rate,

    SUM(shift_diff_hours) AS shift_diff_hours,

    ROUND(SUM(shift_diff_hours) / NULLIF(SUM(hours), 0) * 100, 2) AS shift_diff_pct

FROM business.staffing_by_shift
GROUP BY shift, week_number
ORDER BY week_number, shift;
