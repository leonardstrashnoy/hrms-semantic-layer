-- Metrics view: Executive KPIs
-- Purpose: Dashboard KPIs for headcount, attendance rate, payroll, OT hours

CREATE OR REPLACE VIEW metrics.executive_kpis AS

WITH headcount AS (
    SELECT
        COUNT(DISTINCT employee_int_id) AS total_employees,
        COUNT(DISTINCT CASE WHEN employee_status = 'Active' THEN employee_int_id END) AS active_employees,
        COUNT(DISTINCT CASE WHEN employee_status = 'Terminated' THEN employee_int_id END) AS terminated_employees,
        COUNT(DISTINCT CASE WHEN hire_date >= CURRENT_DATE - INTERVAL '30 days' THEN employee_int_id END) AS new_hires_30d,
        COUNT(DISTINCT department_name) AS total_departments
    FROM staging.stg_workforce
),

attendance_30d AS (
    SELECT
        COUNT(DISTINCT attendance_id) AS total_attendance_records,
        COUNT(DISTINCT employee_int_id) AS employees_with_attendance,
        COUNT(DISTINCT CASE WHEN attendance_status_id = 1 THEN attendance_id END) AS present_days,
        COUNT(DISTINCT CASE WHEN attendance_status_id IN (2, 3, 5) THEN attendance_id END) AS absent_days,
        COUNT(DISTINCT CASE WHEN is_late THEN attendance_id END) AS late_arrivals,
        SUM(COALESCE(ot_duration, 0)) / 60.0 AS total_ot_hours
    FROM staging.stg_daily_attendance
    WHERE attendance_date >= CURRENT_DATE - INTERVAL '30 days'
),

latest_payroll AS (
    SELECT
        pay_date,
        SUM(gross_pay) AS total_gross_pay,
        SUM(net_pay) AS total_net_pay,
        SUM(total_hours) AS total_hours,
        SUM(employer_taxes + employer_amount) AS total_employer_cost,
        COUNT(DISTINCT employee_int_id) AS employees_paid
    FROM staging.stg_payroll_main
    WHERE pay_date = (SELECT MAX(pay_date) FROM staging.stg_payroll_main)
    GROUP BY pay_date
),

payroll_ytd AS (
    SELECT
        SUM(gross_pay) AS gross_pay_ytd,
        SUM(net_pay) AS net_pay_ytd,
        SUM(employer_taxes + employer_amount) AS employer_cost_ytd
    FROM staging.stg_payroll_main
    WHERE EXTRACT(YEAR FROM pay_date) = EXTRACT(YEAR FROM CURRENT_DATE)
),

payroll_trend AS (
    SELECT
        STRFTIME(pay_date, '%Y-%m') AS pay_period,
        SUM(gross_pay) AS period_gross_pay
    FROM staging.stg_payroll_main
    WHERE pay_date >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY STRFTIME(pay_date, '%Y-%m')
    ORDER BY pay_period DESC
    LIMIT 1
)

SELECT
    -- Headcount KPIs
    h.active_employees,
    h.total_employees,
    h.terminated_employees,
    h.new_hires_30d,
    h.total_departments,

    -- Attendance KPIs (30 day)
    a.total_attendance_records AS attendance_records_30d,
    a.present_days,
    a.absent_days,
    a.late_arrivals AS late_arrivals_30d,
    CASE
        WHEN (a.present_days + a.absent_days) > 0
        THEN ROUND(100.0 * a.present_days / (a.present_days + a.absent_days), 1)
        ELSE 0
    END AS attendance_rate_30d,
    ROUND(a.total_ot_hours, 1) AS total_ot_hours_30d,

    -- Latest Payroll KPIs
    lp.pay_date AS latest_pay_date,
    ROUND(lp.total_gross_pay, 2) AS latest_gross_payroll,
    ROUND(lp.total_net_pay, 2) AS latest_net_payroll,
    ROUND(lp.total_employer_cost, 2) AS latest_employer_cost,
    lp.employees_paid AS latest_employees_paid,
    ROUND(lp.total_hours, 1) AS latest_total_hours,

    -- YTD Payroll
    ROUND(pyd.gross_pay_ytd, 2) AS ytd_gross_payroll,
    ROUND(pyd.net_pay_ytd, 2) AS ytd_net_payroll,
    ROUND(pyd.employer_cost_ytd, 2) AS ytd_employer_cost,

    -- Average metrics
    CASE
        WHEN h.active_employees > 0
        THEN ROUND(lp.total_gross_pay / h.active_employees, 2)
        ELSE 0
    END AS avg_gross_pay_per_employee,

    CASE
        WHEN lp.employees_paid > 0
        THEN ROUND(lp.total_hours / lp.employees_paid, 1)
        ELSE 0
    END AS avg_hours_per_employee,

    -- Timestamp
    CURRENT_TIMESTAMP AS calculated_at

FROM headcount h
CROSS JOIN attendance_30d a
CROSS JOIN latest_payroll lp
CROSS JOIN payroll_ytd pyd;
