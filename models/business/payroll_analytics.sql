-- Business view: Payroll Analytics
-- Purpose: Payroll with employee info, effective rates, period analysis

CREATE OR REPLACE VIEW business.payroll_analytics AS

SELECT
    p.payroll_work_id,
    p.employee_int_id,
    w.employee_id,
    w.full_name AS employee_name,
    w.department_name,
    w.department_number,
    w.job_code_name,
    w.employee_type,
    w.employee_status,

    -- Pay period info
    p.timecard_date,
    p.pay_date,
    p.check_date,
    EXTRACT(YEAR FROM p.pay_date) AS pay_year,
    EXTRACT(MONTH FROM p.pay_date) AS pay_month,
    EXTRACT(QUARTER FROM p.pay_date) AS pay_quarter,

    -- Pay period identifier (YYYY-MM format)
    STRFTIME(p.pay_date, '%Y-%m') AS pay_period,

    -- Compensation
    p.gross_pay,
    p.net_pay,
    p.gross_pay_ytd,
    p.deductions,
    p.deductions_ytd,

    -- Taxes
    p.employee_taxes,
    p.employee_taxes_ytd,
    p.employer_taxes,
    p.employer_taxes_ytd,
    p.employer_amount,
    p.employer_amount_ytd,

    -- Total employer cost
    p.gross_pay + p.employer_taxes + p.employer_amount AS total_employer_cost,

    -- Hours
    p.total_hours,
    p.total_hours_ytd,

    -- Effective hourly rate (if hours > 0)
    CASE
        WHEN p.total_hours > 0 THEN ROUND(p.gross_pay / p.total_hours, 2)
        ELSE w.hourly_rate
    END AS effective_hourly_rate,

    -- Base hourly rate from workforce
    w.hourly_rate AS base_hourly_rate,

    -- Rate premium (effective vs base)
    CASE
        WHEN p.total_hours > 0 AND w.hourly_rate > 0 THEN
            ROUND((p.gross_pay / p.total_hours) - w.hourly_rate, 2)
        ELSE 0
    END AS rate_premium,

    -- Payment info
    p.check_number,
    p.check_amount,
    p.direct_deposit_amount,
    p.deposit_type,

    -- Status
    p.payroll_status,
    p.payment_status,
    p.timecard_status,
    p.payroll_type,
    p.payslip_released,

    -- 401k
    p.hours_401k,
    p.hours_401k_ytd,

    -- Audit
    p.created_date,
    p.last_modified_date

FROM staging.stg_payroll_main p
LEFT JOIN staging.stg_workforce w ON p.employee_int_id = w.employee_int_id
WHERE p.pay_date IS NOT NULL;
