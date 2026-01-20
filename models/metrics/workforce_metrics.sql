-- Metrics view: Workforce Metrics
-- Purpose: Department headcount, tenure distribution aggregates

CREATE OR REPLACE VIEW metrics.workforce_metrics AS

WITH dept_metrics AS (
    SELECT
        department_name,
        department_number,

        -- Headcount
        COUNT(DISTINCT employee_int_id) AS total_employees,
        COUNT(DISTINCT CASE WHEN employee_status = 'Active' THEN employee_int_id END) AS active_employees,
        COUNT(DISTINCT CASE WHEN employee_status = 'Terminated' THEN employee_int_id END) AS terminated_employees,

        -- By employee type
        COUNT(DISTINCT CASE WHEN employee_type = 'Full Time' THEN employee_int_id END) AS full_time_count,
        COUNT(DISTINCT CASE WHEN employee_type = 'Part Time' THEN employee_int_id END) AS part_time_count,
        COUNT(DISTINCT CASE WHEN employee_type = 'PRN' THEN employee_int_id END) AS prn_count,

        -- Gender distribution
        COUNT(DISTINCT CASE WHEN gender = 'M' THEN employee_int_id END) AS male_count,
        COUNT(DISTINCT CASE WHEN gender = 'F' THEN employee_int_id END) AS female_count,

        -- New hires
        COUNT(DISTINCT CASE WHEN hire_date >= CURRENT_DATE - INTERVAL '30 days' THEN employee_int_id END) AS new_hires_30d,
        COUNT(DISTINCT CASE WHEN hire_date >= CURRENT_DATE - INTERVAL '90 days' THEN employee_int_id END) AS new_hires_90d,

        -- Tenure stats
        AVG(DATEDIFF('year', hire_date, CURRENT_DATE)) AS avg_tenure_years,
        MIN(DATEDIFF('year', hire_date, CURRENT_DATE)) AS min_tenure_years,
        MAX(DATEDIFF('year', hire_date, CURRENT_DATE)) AS max_tenure_years,

        -- Compensation stats
        AVG(hourly_rate) AS avg_hourly_rate,
        MIN(hourly_rate) AS min_hourly_rate,
        MAX(hourly_rate) AS max_hourly_rate,
        SUM(hourly_rate * standard_weekly_hours * 52) AS total_annual_payroll_estimate,

        -- Leave balances
        SUM(pto_balance) AS total_pto_balance,
        SUM(sick_balance) AS total_sick_balance,
        AVG(pto_balance) AS avg_pto_balance,
        AVG(sick_balance) AS avg_sick_balance

    FROM staging.stg_workforce
    WHERE department_name IS NOT NULL
    GROUP BY department_name, department_number
),

tenure_distribution AS (
    SELECT
        department_name,
        COUNT(DISTINCT CASE WHEN tenure_band = '< 1 Year' THEN employee_int_id END) AS tenure_under_1yr,
        COUNT(DISTINCT CASE WHEN tenure_band = '1-2 Years' THEN employee_int_id END) AS tenure_1_2yr,
        COUNT(DISTINCT CASE WHEN tenure_band = '3-4 Years' THEN employee_int_id END) AS tenure_3_4yr,
        COUNT(DISTINCT CASE WHEN tenure_band = '5-9 Years' THEN employee_int_id END) AS tenure_5_9yr,
        COUNT(DISTINCT CASE WHEN tenure_band = '10-19 Years' THEN employee_int_id END) AS tenure_10_19yr,
        COUNT(DISTINCT CASE WHEN tenure_band = '20+ Years' THEN employee_int_id END) AS tenure_20plus
    FROM business.workforce_demographics
    GROUP BY department_name
),

age_distribution AS (
    SELECT
        department_name,
        COUNT(DISTINCT CASE WHEN age_band = 'Under 25' THEN employee_int_id END) AS age_under_25,
        COUNT(DISTINCT CASE WHEN age_band = '25-34' THEN employee_int_id END) AS age_25_34,
        COUNT(DISTINCT CASE WHEN age_band = '35-44' THEN employee_int_id END) AS age_35_44,
        COUNT(DISTINCT CASE WHEN age_band = '45-54' THEN employee_int_id END) AS age_45_54,
        COUNT(DISTINCT CASE WHEN age_band = '55-64' THEN employee_int_id END) AS age_55_64,
        COUNT(DISTINCT CASE WHEN age_band = '65+' THEN employee_int_id END) AS age_65plus
    FROM business.workforce_demographics
    GROUP BY department_name
)

SELECT
    d.*,
    t.tenure_under_1yr,
    t.tenure_1_2yr,
    t.tenure_3_4yr,
    t.tenure_5_9yr,
    t.tenure_10_19yr,
    t.tenure_20plus,
    a.age_under_25,
    a.age_25_34,
    a.age_35_44,
    a.age_45_54,
    a.age_55_64,
    a.age_65plus,
    CURRENT_TIMESTAMP AS calculated_at

FROM dept_metrics d
LEFT JOIN tenure_distribution t ON d.department_name = t.department_name
LEFT JOIN age_distribution a ON d.department_name = a.department_name
ORDER BY d.active_employees DESC;
