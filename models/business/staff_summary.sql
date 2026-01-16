-- Business view: Clinical Staff Summary
-- Summary of employee benefits enrollment

CREATE OR REPLACE VIEW business.clinical_staff_summary AS

SELECT
    employee_id,
    employee_int_id,
    full_name,
    first_name,
    last_name,

    num_benefit_plans,
    benefit_plan_types,
    total_current_arrears,
    total_arrears,

    corp_id,
    last_updated

FROM business.employee_summary
ORDER BY full_name;
