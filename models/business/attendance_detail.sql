-- Business view: Attendance Detail
-- Provides detailed attendance records

CREATE OR REPLACE VIEW business.attendance_detail AS

SELECT
    employee_id,
    employee_int_id,
    first_name,
    last_name,
    first_name || ' ' || last_name AS full_name,

    week_number,
    shift,
    total_hours,
    hours,
    rate,
    earning_code,
    earning_number,
    job_code,
    dept_id,
    shift_diff_flag,

    source_table,
    _loaded_at AS last_updated

FROM staging.stg_attendance
WHERE employee_id IS NOT NULL
ORDER BY employee_id, week_number;
