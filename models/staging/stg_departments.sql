-- Staging view for department data
-- Source: raw.tppd_dept (185 rows)
-- Purpose: Department hierarchy and metadata

CREATE OR REPLACE VIEW staging.stg_departments AS

SELECT
    deptint_id AS department_int_id,
    corp_id AS corp_id,
    TRIM(corp_mnec) AS corp_code,
    TRIM(dept_mnec) AS department_code,
    TRIM(dept_num) AS department_number,
    TRIM(dept_name) AS department_name,

    -- Classification
    CAST(class AS INTEGER) AS department_class,
    CAST(type AS INTEGER) AS department_type,
    CAST(pattern AS INTEGER) AS department_pattern,

    -- Status
    CASE
        WHEN UPPER(TRIM(active_yn)) IN ('Y', 'YES', 'TRUE', '1') THEN TRUE
        ELSE FALSE
    END AS is_active,

    -- Lunch policy
    COALESCE(Lunch_Break_Mins, 30) AS lunch_break_mins,

    -- Audit fields
    created_dt AS created_date,
    created_by,
    lst_mod_dt AS last_modified_date,
    lst_mod_user AS last_modified_by,

    -- Metadata
    'tppd_dept' AS source_table,
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.tppd_dept
WHERE dept_name IS NOT NULL
  AND TRIM(dept_name) != '';
