-- Business view: Staffing by Shift
-- Analyzes employee hours by shift

CREATE OR REPLACE VIEW business.staffing_by_shift AS

SELECT
    employee_id,
    first_name,
    last_name,
    first_name || ' ' || last_name AS full_name,

    shift,
    week_number,
    dept_id,
    job_code,

    SUM(total_hours) AS total_hours,
    SUM(hours) AS hours,
    AVG(rate) AS avg_rate,

    SUM(CASE WHEN shift_diff_flag THEN hours ELSE 0 END) AS shift_diff_hours,

    COUNT(*) AS record_count

FROM staging.stg_attendance
WHERE employee_id IS NOT NULL
GROUP BY employee_id, first_name, last_name, shift, week_number, dept_id, job_code
ORDER BY employee_id, week_number, shift;
