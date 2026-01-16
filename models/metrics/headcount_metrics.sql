-- Metrics view: Headcount/Employee Metrics
-- Employee counts based on payroll data

CREATE OR REPLACE VIEW metrics.headcount_metrics AS

SELECT
    corp_id,
    benefit_plan_type,

    COUNT(DISTINCT employee_id) AS employee_count,
    COUNT(*) AS enrollment_records,

    -- Arrears summary
    SUM(current_arrears) AS total_current_arrears,
    SUM(total_arrears) AS total_arrears,
    ROUND(AVG(current_arrears), 2) AS avg_current_arrears,

    -- Age metrics (if available)
    ROUND(AVG(age), 1) AS avg_age

FROM staging.stg_payroll
WHERE employee_id IS NOT NULL
GROUP BY corp_id, benefit_plan_type
ORDER BY employee_count DESC;
