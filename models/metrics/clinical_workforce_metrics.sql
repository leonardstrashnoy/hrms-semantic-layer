-- Metrics view: Workforce Summary Metrics
-- Summary statistics from employee data

CREATE OR REPLACE VIEW metrics.clinical_workforce_metrics AS

SELECT
    corp_id,

    COUNT(DISTINCT employee_id) AS employee_count,

    SUM(total_current_arrears) AS total_current_arrears,
    SUM(total_arrears) AS total_arrears,
    ROUND(AVG(num_benefit_plans), 2) AS avg_benefit_plans_per_employee

FROM business.employee_summary
GROUP BY corp_id
ORDER BY employee_count DESC;
