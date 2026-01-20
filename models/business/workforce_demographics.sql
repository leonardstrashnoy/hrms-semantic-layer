-- Business view: Workforce Demographics
-- Purpose: Employee data with tenure/age bands, direct reports count

CREATE OR REPLACE VIEW business.workforce_demographics AS

WITH direct_reports AS (
    -- Count direct reports for each manager
    SELECT
        reporting_manager,
        COUNT(DISTINCT employee_int_id) AS direct_report_count
    FROM staging.stg_workforce
    WHERE employee_status = 'Active'
    GROUP BY reporting_manager
),

employee_data AS (
    SELECT
        w.employee_int_id,
        w.employee_id,
        w.first_name,
        w.last_name,
        w.full_name,
        w.employee_type,
        w.employee_status,
        w.gender,
        w.hire_date,
        w.date_of_birth,

        -- Department info
        w.department_number,
        w.department_name,
        w.dept_manager,
        w.reporting_manager,

        -- Job info
        w.job_code,
        w.job_code_name,
        w.standard_weekly_hours,
        w.hourly_rate,
        w.flsa_exempt,
        w.nurse_type,
        w.payroll_type,

        -- Calculate tenure
        CASE
            WHEN w.hire_date IS NOT NULL THEN
                DATEDIFF('year', w.hire_date, CURRENT_DATE)
            ELSE NULL
        END AS tenure_years,

        -- Calculate age
        CASE
            WHEN w.date_of_birth IS NOT NULL THEN
                DATEDIFF('year', w.date_of_birth, CURRENT_DATE)
            ELSE NULL
        END AS age,

        -- Leave balances
        w.pto_balance,
        w.sick_balance

    FROM staging.stg_workforce w
)

SELECT
    e.*,

    -- Tenure bands
    CASE
        WHEN e.tenure_years IS NULL THEN 'Unknown'
        WHEN e.tenure_years < 1 THEN '< 1 Year'
        WHEN e.tenure_years < 3 THEN '1-2 Years'
        WHEN e.tenure_years < 5 THEN '3-4 Years'
        WHEN e.tenure_years < 10 THEN '5-9 Years'
        WHEN e.tenure_years < 20 THEN '10-19 Years'
        ELSE '20+ Years'
    END AS tenure_band,

    -- Age bands
    CASE
        WHEN e.age IS NULL THEN 'Unknown'
        WHEN e.age < 25 THEN 'Under 25'
        WHEN e.age < 35 THEN '25-34'
        WHEN e.age < 45 THEN '35-44'
        WHEN e.age < 55 THEN '45-54'
        WHEN e.age < 65 THEN '55-64'
        ELSE '65+'
    END AS age_band,

    -- Direct reports (for managers)
    COALESCE(dr.direct_report_count, 0) AS direct_reports,

    -- Manager span classification
    CASE
        WHEN COALESCE(dr.direct_report_count, 0) = 0 THEN 'Individual Contributor'
        WHEN dr.direct_report_count <= 5 THEN 'Small Team (1-5)'
        WHEN dr.direct_report_count <= 10 THEN 'Medium Team (6-10)'
        WHEN dr.direct_report_count <= 20 THEN 'Large Team (11-20)'
        ELSE 'Very Large Team (20+)'
    END AS manager_span,

    -- Annual salary estimate
    CASE
        WHEN e.hourly_rate > 0 THEN
            e.hourly_rate * e.standard_weekly_hours * 52
        ELSE 0
    END AS estimated_annual_salary

FROM employee_data e
LEFT JOIN direct_reports dr ON e.full_name = dr.reporting_manager;
