-- Metrics view: Payroll/Benefits Metrics
-- Aggregated benefits enrollment metrics

CREATE OR REPLACE VIEW metrics.monthly_payroll_metrics AS

SELECT
    corp_id,
    benefit_plan_type,
    benefit_plan_name,
    benefit_class_name,

    COUNT(DISTINCT employee_id) AS employee_count,
    COUNT(*) AS enrollment_records,

    -- Arrears
    SUM(current_arrears) AS total_current_arrears,
    SUM(total_arrears) AS total_arrears,
    COUNT(CASE WHEN arrears_assigned THEN 1 END) AS arrears_assigned_count,

    -- Pay periods
    SUM(num_pay_periods) AS total_pay_periods

FROM staging.stg_payroll
WHERE employee_id IS NOT NULL
GROUP BY corp_id, benefit_plan_type, benefit_plan_name, benefit_class_name
ORDER BY employee_count DESC;
