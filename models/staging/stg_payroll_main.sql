-- Staging view for payroll main data
-- Source: raw.tpp_tc_main (77K rows)
-- Purpose: Gross/net pay, deductions per period

CREATE OR REPLACE VIEW staging.stg_payroll_main AS

SELECT
    tcpaywrk_int_id AS payroll_work_id,
    tcpaydt_int_id AS payroll_date_id,
    emp_int_id AS employee_int_id,

    -- Pay period info
    tc_dt AS timecard_date,
    pay_dt AS pay_date,
    chk_dt AS check_date,

    -- Compensation amounts
    COALESCE(gross_pay, 0) AS gross_pay,
    COALESCE(gross_pay_ytd, 0) AS gross_pay_ytd,
    COALESCE(net_pay, 0) AS net_pay,

    -- Deductions
    COALESCE(deductions, 0) AS deductions,
    COALESCE(deductions_ytd, 0) AS deductions_ytd,

    -- Taxes
    COALESCE(emptaxes, 0) AS employee_taxes,
    COALESCE(emptaxes_ytd, 0) AS employee_taxes_ytd,
    COALESCE(empr_taxes, 0) AS employer_taxes,
    COALESCE(emprtaxes_ytd, 0) AS employer_taxes_ytd,

    -- Employer contributions
    COALESCE(empr_amt, 0) AS employer_amount,
    COALESCE(empr_amtytd, 0) AS employer_amount_ytd,

    -- Hours
    COALESCE(total_hrs, 0) AS total_hours,
    COALESCE(total_hrs_ytd, 0) AS total_hours_ytd,
    COALESCE(hrs_401k, 0) AS hours_401k,
    COALESCE(hrs_401k_ytd, 0) AS hours_401k_ytd,

    -- Check info
    TRIM(check_num) AS check_number,
    TRIM(check_comment) AS check_comment,
    COALESCE(check_amt, 0) AS check_amount,
    COALESCE(ddeposit_amt, 0) AS direct_deposit_amount,
    CAST(deposit_type AS INTEGER) AS deposit_type,

    -- Status flags
    pyrl_sts AS payroll_status,
    paymt_sts AS payment_status,
    paymt_sts_dt AS payment_status_date,
    tc_sts AS timecard_status,
    pyrl_type AS payroll_type,
    emp_pyrl_type AS employee_payroll_type,

    -- Boolean flags
    COALESCE(flsa_mthd_yn, FALSE) AS flsa_method,
    COALESCE(esign_yn, FALSE) AS has_esign,
    COALESCE(ach_genrated_yn, FALSE) AS ach_generated,
    COALESCE(ReleasePayslip, FALSE) AS payslip_released,

    -- Additional info
    TRIM(pyrl_comments) AS payroll_comments,
    TRIM(bank_mnec) AS bank_code,

    -- Audit fields
    created_dt AS created_date,
    lst_mod_dt AS last_modified_date,

    -- Metadata
    'tpp_tc_main' AS source_table,
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.tpp_tc_main
WHERE emp_int_id IS NOT NULL;
