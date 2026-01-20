-- Business view: Daily Attendance Detail
-- Purpose: Attendance records with employee names and status descriptions

CREATE OR REPLACE VIEW business.daily_attendance_detail AS

SELECT
    a.attendance_id,
    a.employee_int_id,
    w.employee_id,
    w.full_name AS employee_name,
    w.department_name,
    w.department_number,
    w.job_code_name,

    -- Attendance date/time
    a.attendance_date,
    EXTRACT(YEAR FROM a.attendance_date) AS attendance_year,
    EXTRACT(MONTH FROM a.attendance_date) AS attendance_month,
    EXTRACT(DOW FROM a.attendance_date) AS day_of_week,
    CASE EXTRACT(DOW FROM a.attendance_date)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,

    -- Time details
    a.scheduled_in_time,
    a.actual_in_time,
    a.out_time,
    a.planned_end_time,

    -- Attendance status description
    CASE a.attendance_status_id
        WHEN 1 THEN 'Present'
        WHEN 2 THEN 'Absent'
        WHEN 3 THEN 'Leave'
        WHEN 4 THEN 'Holiday'
        WHEN 5 THEN 'Half Day'
        WHEN 6 THEN 'Weekend'
        WHEN 7 THEN 'Work From Home'
        ELSE 'Unknown'
    END AS attendance_status,

    -- Half-day status
    a.first_half_status,
    a.second_half_status,

    -- Late/Early flags
    a.is_late,
    a.late_by_mins,
    a.is_early_leave,
    a.early_left_by_mins,
    a.is_absent,
    a.absent_minutes,

    -- Duration in hours (convert from minutes)
    ROUND(a.total_duration / 60.0, 2) AS total_hours,
    ROUND(a.total_net_duration / 60.0, 2) AS net_hours,
    ROUND(a.pay_duration / 60.0, 2) AS pay_hours,
    ROUND(a.shift_work_duration / 60.0, 2) AS shift_hours,

    -- Overtime
    a.has_overtime,
    a.ot_start_time,
    a.ot_end_time,
    ROUND(a.ot_duration / 60.0, 2) AS ot_hours,
    ROUND(a.ot_net_duration / 60.0, 2) AS ot_net_hours,

    -- Break time in hours
    ROUND(a.total_break_mins / 60.0, 2) AS total_break_hours,

    -- On-call
    ROUND(a.on_call_duration / 60.0, 2) AS on_call_hours,
    ROUND(a.call_back_duration / 60.0, 2) AS call_back_hours,

    -- Time management code
    a.time_mgt_code,

    -- Audit
    a.hr_flag,
    a.entered_date

FROM staging.stg_daily_attendance a
LEFT JOIN staging.stg_workforce w ON a.employee_int_id = w.employee_int_id
WHERE a.attendance_date IS NOT NULL;
