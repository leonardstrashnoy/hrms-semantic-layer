-- Staging view for attendance data
-- Uses actual column names from Attendance tables

CREATE OR REPLACE VIEW staging.stg_attendance AS

SELECT
    CAST(Emp_ID AS VARCHAR) AS employee_id,
    CAST(emp_int_id AS VARCHAR) AS employee_int_id,
    TRIM("First Name") AS first_name,
    TRIM("Last Name") AS last_name,

    CAST("Week Number" AS INTEGER) AS week_number,
    TRIM(Shift) AS shift,
    CAST(shift_int_id AS INTEGER) AS shift_int_id,

    CAST("Total Hours" AS DECIMAL(8, 2)) AS total_hours,
    CAST(hours AS DECIMAL(8, 2)) AS hours,
    CAST(RATE AS DECIMAL(10, 2)) AS rate,

    TRIM("Earning Code") AS earning_code,
    TRIM(earning_number) AS earning_number,

    CAST("Job Code" AS INTEGER) AS job_code,
    CAST("Dept ID" AS INTEGER) AS dept_id,

    CAST(shiff_diff_yn AS BOOLEAN) AS shift_diff_flag,

    -- Metadata
    'attendance_031820' AS source_table,
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.attendance_031820
WHERE Emp_ID IS NOT NULL

UNION ALL

SELECT
    CAST(Emp_ID AS VARCHAR) AS employee_id,
    CAST(emp_int_id AS VARCHAR) AS employee_int_id,
    TRIM("First Name") AS first_name,
    TRIM("Last Name") AS last_name,

    CAST("Week Number" AS INTEGER) AS week_number,
    TRIM(Shift) AS shift,
    CAST(shift_int_id AS INTEGER) AS shift_int_id,

    CAST("Total Hours" AS DECIMAL(8, 2)) AS total_hours,
    CAST(hours AS DECIMAL(8, 2)) AS hours,
    CAST(RATE AS DECIMAL(10, 2)) AS rate,

    TRIM("Earning Code") AS earning_code,
    TRIM(earning_number) AS earning_number,

    CAST("Job Code" AS INTEGER) AS job_code,
    CAST("Dept ID" AS INTEGER) AS dept_id,

    CAST(shiff_diff_yn AS BOOLEAN) AS shift_diff_flag,

    'attendance_031920' AS source_table,
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.attendance_031920
WHERE Emp_ID IS NOT NULL;
