-- Staging view for payroll/benefits data
-- Uses actual column names from CRMC_PayrollFile table

CREATE OR REPLACE VIEW staging.stg_payroll AS

SELECT
    CAST("Employee ID" AS VARCHAR) AS employee_id,
    CAST(emp_Int_id AS VARCHAR) AS employee_int_id,
    TRIM("First Name") AS first_name,
    TRIM("Last Name") AS last_name,

    -- Benefits information
    TRIM("Benefit Plan Type") AS benefit_plan_type,
    TRIM("Benefit Plan Name") AS benefit_plan_name,
    TRIM("Benefit Class Name") AS benefit_class_name,
    TRIM("Coverage Tier Name (No Codes)") AS coverage_tier,
    TRIM("Relationship (No Codes)") AS relationship,

    -- Cost information
    TRIM("Cost Tier Effective Date") AS cost_tier_effective_date,
    TRIM("Coverage Amount 1") AS coverage_amount_1,
    TRIM("Coverage Amount 2") AS coverage_amount_2,
    TRIM("Per Pay Period Amount 1") AS per_pay_period_amount_1,
    TRIM("Per Pay Period Amount 2") AS per_pay_period_amount_2,
    TRIM("Employer Cost") AS employer_cost,
    TRIM("Employer Cost Per Pay Period") AS employer_cost_per_pay_period,

    -- Arrears
    CAST("Current Arrears" AS DECIMAL(12, 2)) AS current_arrears,
    CAST("Total Arrears" AS DECIMAL(12, 2)) AS total_arrears,
    CAST("No of PP" AS INTEGER) AS num_pay_periods,
    CAST(arrears_assigned_yn AS BOOLEAN) AS arrears_assigned,

    -- Other info
    TRIM(Type) AS record_type,
    TRIM(Reason) AS reason,
    CAST(Age AS INTEGER) AS age,
    CAST(Corp_Id AS INTEGER) AS corp_id,

    -- Metadata
    'crmc_payrollfile' AS source_table,
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.crmc_payrollfile
WHERE "Employee ID" IS NOT NULL;
