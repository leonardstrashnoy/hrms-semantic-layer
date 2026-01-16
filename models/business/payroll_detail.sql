-- Business view: Payroll/Benefits Detail
-- Provides detailed benefits and cost information

CREATE OR REPLACE VIEW business.payroll_detail AS

SELECT
    employee_id,
    employee_int_id,
    first_name,
    last_name,
    first_name || ' ' || last_name AS full_name,

    -- Benefits information
    benefit_plan_type,
    benefit_plan_name,
    benefit_class_name,
    coverage_tier,
    relationship,

    -- Cost information
    cost_tier_effective_date,
    coverage_amount_1,
    coverage_amount_2,
    per_pay_period_amount_1,
    per_pay_period_amount_2,
    employer_cost,
    employer_cost_per_pay_period,

    -- Arrears
    current_arrears,
    total_arrears,
    num_pay_periods,
    arrears_assigned,

    -- Other info
    record_type,
    reason,
    age,
    corp_id,

    source_table,
    _loaded_at AS last_updated

FROM staging.stg_payroll
WHERE employee_id IS NOT NULL
ORDER BY last_name, first_name, benefit_plan_type;
