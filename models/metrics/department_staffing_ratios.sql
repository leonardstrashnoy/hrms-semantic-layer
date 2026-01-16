-- Metrics view: Department Staffing
-- Staffing metrics by department and shift

CREATE OR REPLACE VIEW metrics.department_staffing_ratios AS

SELECT
    dept_id,
    shift,
    week_number,

    SUM(record_count) AS total_records,
    COUNT(DISTINCT employee_id) AS unique_employees,

    SUM(total_hours) AS total_hours,
    SUM(hours) AS hours_worked,
    ROUND(AVG(total_hours), 2) AS avg_hours,

    SUM(shift_diff_hours) AS shift_diff_hours

FROM business.staffing_by_shift
GROUP BY dept_id, shift, week_number
ORDER BY dept_id, week_number, shift;
