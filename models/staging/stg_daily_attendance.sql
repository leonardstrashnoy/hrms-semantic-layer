-- Staging view for daily attendance data
-- Source: raw.thr_attendance (395K rows)
-- Purpose: Daily punch times, late/absent flags

CREATE OR REPLACE VIEW staging.stg_daily_attendance AS

SELECT
    Att_Int_Id AS attendance_id,
    Emp_Int_Id AS employee_int_id,
    CAST(Shift_Id AS BIGINT) AS shift_id,

    -- Attendance timing
    Att_For_Date AS attendance_date,
    Actual_In_Time AS actual_in_time,
    In_Time AS scheduled_in_time,
    Out_Time AS out_time,
    Planned_End_Time AS planned_end_time,

    -- Status flags
    Att_Status_Id AS attendance_status_id,
    Att_Status_Id_Original AS original_status_id,
    TRIM(First_Half_Status) AS first_half_status,
    TRIM(Second_Half_Status) AS second_half_status,
    TRIM(Time_Mgt_Code) AS time_mgt_code,

    -- Late/Early tracking
    COALESCE(Late_By_Mins, 0) AS late_by_mins,
    COALESCE(Early_Left_By_Mins, 0) AS early_left_by_mins,
    COALESCE(Absent_Minutes, 0) AS absent_minutes,

    -- Duration calculations
    COALESCE(Shift_Work_Duration, 0) AS shift_work_duration,
    COALESCE(Shift_Work_Net_Duration, 0) AS shift_work_net_duration,
    COALESCE(Total_Duration, 0) AS total_duration,
    COALESCE(Total_Net_Duration, 0) AS total_net_duration,
    COALESCE(Pay_Duration, 0) AS pay_duration,

    -- OT tracking
    OT_Start_Time AS ot_start_time,
    OT_End_Time AS ot_end_time,
    COALESCE(OT_Duration, 0) AS ot_duration,
    COALESCE(OT_Net_Duration, 0) AS ot_net_duration,
    COALESCE(OT_Duration_Actual, 0) AS ot_actual_duration,

    -- Break tracking
    COALESCE(Shift_Allowed_Break_Duration, 0) AS allowed_break_mins,
    COALESCE(Shift_Break_Duration, 0) AS shift_break_mins,
    COALESCE(Taken_Paid_Break_Duration, 0) AS taken_paid_break_mins,
    COALESCE(Taken_Un_Paid_Break_Duration, 0) AS taken_unpaid_break_mins,
    COALESCE(Total_Break_Duration, 0) AS total_break_mins,

    -- Call tracking
    COALESCE(On_Call_Duration, 0) AS on_call_duration,
    COALESCE(Call_Back_Duration, 0) AS call_back_duration,

    -- Derived flags
    CASE WHEN Late_By_Mins > 0 THEN TRUE ELSE FALSE END AS is_late,
    CASE WHEN Early_Left_By_Mins > 0 THEN TRUE ELSE FALSE END AS is_early_leave,
    CASE WHEN Absent_Minutes > 0 OR Att_Status_Id IN (2, 3, 5) THEN TRUE ELSE FALSE END AS is_absent,
    CASE WHEN OT_Duration > 0 THEN TRUE ELSE FALSE END AS has_overtime,

    -- Audit fields
    HR_Flag AS hr_flag,
    CAST(Entered_By AS BIGINT) AS entered_by,
    Entered_Date AS entered_date,

    -- Metadata
    'thr_attendance' AS source_table,
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.thr_attendance
WHERE Att_For_Date IS NOT NULL;
