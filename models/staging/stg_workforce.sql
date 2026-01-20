-- Staging view for workforce data
-- Source: raw.workforcetable_rpt (115K rows)
-- Purpose: Employee master with hire date, department, manager, salary

CREATE OR REPLACE VIEW staging.stg_workforce AS

SELECT
    CAST(emp_int_id AS BIGINT) AS employee_int_id,
    TRIM("EmployeeId") AS employee_id,
    TRIM("First Name") AS first_name,
    TRIM("Last Name") AS last_name,
    TRIM("First Name") || ' ' || TRIM("Last Name") AS full_name,

    -- Employment info
    TRIM("EmployeeType") AS employee_type,
    TRIM("EmployeeStatus") AS employee_status,
    TRY_CAST("DOB" AS DATE) AS date_of_birth,
    TRY_CAST("HireDate" AS DATE) AS hire_date,
    TRY_CAST("StatusDate" AS DATE) AS status_date,
    TRIM("Gender") AS gender,

    -- Department info
    TRIM("DepartmentNumber") AS department_number,
    TRIM("DepartmentName") AS department_name,
    TRIM("Dept Manager") AS dept_manager,
    TRIM("ReportingManager") AS reporting_manager,
    TRIM("Time Card Approver") AS time_card_approver,

    -- Job info
    TRIM("JobCode") AS job_code,
    TRIM("JobCodeName") AS job_code_name,
    COALESCE(standrd_wk_hours, 40.0) AS standard_weekly_hours,
    shift_hrs AS shift_hours,
    hrs_define_fte AS hours_define_fte,

    -- Classification
    TRIM("FLSAExempt") AS flsa_exempt,
    TRIM("OVTApplicable") AS ovt_applicable,
    TRIM("ShiftDiffApplicable") AS shift_diff_applicable,
    TRIM("Attendance Exemption") AS attendance_exemption,
    TRIM("exempttype_desc") AS exempt_type_desc,
    TRIM("NurseType") AS nurse_type,
    TRIM("Payroll Type") AS payroll_type,

    -- Compensation
    COALESCE(Emprate, 0) AS hourly_rate,

    -- Leave balances
    COALESCE(PTO, 0) AS pto_balance,
    COALESCE(SICK, 0) AS sick_balance,
    COALESCE(CPTO, 0) AS cpto_balance,
    COALESCE(SICK2, 0) AS sick2_balance,

    -- Internal ID for joins
    tcpaydt_int_id,

    -- Metadata
    'workforcetable_rpt' AS source_table,
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.workforcetable_rpt
WHERE "EmployeeId" IS NOT NULL
  AND TRIM("EmployeeId") != '';
