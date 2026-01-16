-- Business view: Employee Summary from Payroll/Benefits data
-- Provides summary of employees based on available payroll data

CREATE OR REPLACE VIEW business.employee_summary AS

SELECT
    employee_id,
    employee_int_id,
    first_name,
    last_name,
    first_name || ' ' || last_name AS full_name,

    -- Benefits summary
    COUNT(DISTINCT benefit_plan_name) AS num_benefit_plans,
    STRING_AGG(DISTINCT benefit_plan_type, ', ') AS benefit_plan_types,

    -- Cost summary
    SUM(current_arrears) AS total_current_arrears,
    SUM(total_arrears) AS total_arrears,

    MAX(corp_id) AS corp_id,
    MAX(_loaded_at) AS last_updated

FROM staging.stg_payroll
WHERE employee_id IS NOT NULL
GROUP BY employee_id, employee_int_id, first_name, last_name;
