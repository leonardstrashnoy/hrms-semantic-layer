-- Metrics view: Payroll Period Metrics
-- Purpose: Payroll totals by period and department

CREATE OR REPLACE VIEW metrics.payroll_period_metrics AS

WITH period_dept_payroll AS (
    SELECT
        STRFTIME(p.pay_date, '%Y-%m') AS pay_period,
        EXTRACT(YEAR FROM p.pay_date) AS pay_year,
        EXTRACT(MONTH FROM p.pay_date) AS pay_month,
        EXTRACT(QUARTER FROM p.pay_date) AS pay_quarter,
        w.department_name,
        w.department_number,

        -- Counts
        COUNT(DISTINCT p.payroll_work_id) AS payroll_records,
        COUNT(DISTINCT p.employee_int_id) AS employees_paid,

        -- Gross/Net
        SUM(p.gross_pay) AS total_gross_pay,
        SUM(p.net_pay) AS total_net_pay,
        AVG(p.gross_pay) AS avg_gross_pay,
        AVG(p.net_pay) AS avg_net_pay,
        MIN(p.gross_pay) AS min_gross_pay,
        MAX(p.gross_pay) AS max_gross_pay,

        -- Deductions and taxes
        SUM(p.deductions) AS total_deductions,
        SUM(p.employee_taxes) AS total_employee_taxes,
        SUM(p.employer_taxes) AS total_employer_taxes,
        SUM(p.employer_amount) AS total_employer_contributions,

        -- Total employer cost
        SUM(p.gross_pay + p.employer_taxes + p.employer_amount) AS total_employer_cost,

        -- Hours
        SUM(p.total_hours) AS total_hours,
        AVG(p.total_hours) AS avg_hours_per_employee,

        -- 401k
        SUM(p.hours_401k) AS total_401k_hours,

        -- Payment methods
        SUM(p.check_amount) AS total_check_amount,
        SUM(p.direct_deposit_amount) AS total_dd_amount,
        COUNT(DISTINCT CASE WHEN p.check_amount > 0 THEN p.employee_int_id END) AS check_payments,
        COUNT(DISTINCT CASE WHEN p.direct_deposit_amount > 0 THEN p.employee_int_id END) AS dd_payments

    FROM staging.stg_payroll_main p
    LEFT JOIN staging.stg_workforce w ON p.employee_int_id = w.employee_int_id
    WHERE p.pay_date IS NOT NULL
    GROUP BY
        STRFTIME(p.pay_date, '%Y-%m'),
        EXTRACT(YEAR FROM p.pay_date),
        EXTRACT(MONTH FROM p.pay_date),
        EXTRACT(QUARTER FROM p.pay_date),
        w.department_name,
        w.department_number
)

SELECT
    pay_period,
    pay_year,
    pay_month,
    pay_quarter,
    department_name,
    department_number,

    -- Counts
    payroll_records,
    employees_paid,

    -- Compensation
    ROUND(total_gross_pay, 2) AS total_gross_pay,
    ROUND(total_net_pay, 2) AS total_net_pay,
    ROUND(avg_gross_pay, 2) AS avg_gross_pay,
    ROUND(avg_net_pay, 2) AS avg_net_pay,
    ROUND(min_gross_pay, 2) AS min_gross_pay,
    ROUND(max_gross_pay, 2) AS max_gross_pay,

    -- Employer costs
    ROUND(total_deductions, 2) AS total_deductions,
    ROUND(total_employee_taxes, 2) AS total_employee_taxes,
    ROUND(total_employer_taxes, 2) AS total_employer_taxes,
    ROUND(total_employer_contributions, 2) AS total_employer_contributions,
    ROUND(total_employer_cost, 2) AS total_employer_cost,

    -- Cost per employee
    CASE
        WHEN employees_paid > 0
        THEN ROUND(total_employer_cost / employees_paid, 2)
        ELSE 0
    END AS avg_employer_cost_per_employee,

    -- Hours
    ROUND(total_hours, 1) AS total_hours,
    ROUND(avg_hours_per_employee, 1) AS avg_hours_per_employee,

    -- Effective hourly rate
    CASE
        WHEN total_hours > 0
        THEN ROUND(total_gross_pay / total_hours, 2)
        ELSE 0
    END AS effective_hourly_rate,

    -- 401k
    ROUND(total_401k_hours, 1) AS total_401k_hours,

    -- Payment distribution
    ROUND(total_check_amount, 2) AS total_check_amount,
    ROUND(total_dd_amount, 2) AS total_dd_amount,
    check_payments,
    dd_payments,
    CASE
        WHEN employees_paid > 0
        THEN ROUND(100.0 * dd_payments / employees_paid, 1)
        ELSE 0
    END AS dd_adoption_rate,

    CURRENT_TIMESTAMP AS calculated_at

FROM period_dept_payroll
ORDER BY pay_period DESC, department_name;
