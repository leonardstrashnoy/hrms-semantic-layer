-- Staging view for activity logs
-- Uses actual column names from Activity_Log table

CREATE OR REPLACE VIEW staging.stg_activity_log AS

SELECT
    CAST(Activity_Id AS VARCHAR) AS activity_id,
    CAST(Emp_Int_ID AS VARCHAR) AS employee_id,
    CAST(EnteredDate AS TIMESTAMP) AS activity_timestamp,

    CAST(Activity_Type AS VARCHAR) AS activity_type,
    CAST(Activity_Desc AS VARCHAR) AS activity_description,

    CAST(ModuleName AS VARCHAR) AS module,
    CAST(EffectedTableName AS VARCHAR) AS affected_table,
    CAST(EffectedColumnName AS VARCHAR) AS affected_column,

    CAST(EnterBy AS VARCHAR) AS entered_by,
    CAST(User_Role AS VARCHAR) AS user_role,

    CAST(StartTime AS TIMESTAMP) AS start_time,
    CAST(EndTime AS TIMESTAMP) AS end_time,

    CAST(SystemIP AS VARCHAR) AS system_ip,
    CAST(PublicIP AS VARCHAR) AS public_ip,

    CAST(Corp_ID AS INTEGER) AS corp_id,

    -- Metadata
    'activity_log' AS source_table,
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.activity_log
WHERE Activity_Id IS NOT NULL;
