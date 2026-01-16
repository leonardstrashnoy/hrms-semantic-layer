-- Staging view for employee data
-- NOTE: The workforce data table wasn't imported (doesn't exist in SQL Server)
-- This view uses payroll data as a substitute for employee information

CREATE OR REPLACE VIEW staging.stg_employees AS
SELECT DISTINCT
    "Employee ID" AS employee_id,
    emp_Int_id AS employee_int_id,
    "First Name" AS first_name,
    "Last Name" AS last_name,
    Corp_Id AS corp_id,
    'crmc_payrollfile' AS source_table,
    CURRENT_TIMESTAMP AS _loaded_at
FROM raw.crmc_payrollfile
WHERE "Employee ID" IS NOT NULL;
