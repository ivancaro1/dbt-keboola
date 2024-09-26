{{ config(
    materialized='table'
) }}

WITH qbml AS (
    SELECT DISTINCT ml_project_name, service_line 
    FROM {{ source('in.c-keboola-ex-onedrive-1176997053', 'REF_QB_ML_PROJECT_SL_MAPPING') }} 
)
SELECT 
    CONCAT('MAVENLINK | ',"id") AS project_key,
    IFNULL(NULLIF("id", ''), '-1') AS project_id,
    IFNULL(NULLIF("title", ''), '$N/A') AS project_name,
    NULLIF("start_date", '') AS project_start_date,
    IFNULL(NULLIF("status_key", '')::INT, '-1') AS project_status_id,
    IFNULL(NULLIF(CONCAT(LEFT(HEX_ENCODE(qbml.service_line)::varchar, 10), RIGHT(HEX_ENCODE(qbml.service_line)::varchar, 10)), ''), '-1') AS service_line_id,
    TRY_TO_DATE("updated_at") AS row_valid_from,
    TO_DATE('9999-12-31', 'YYYY-MM-DD') AS row_valid_to,
    'C' AS row_status,
    False AS is_deleted,
    'MAVENLINK' AS source_system
FROM {{ source('in.c-cuesta-ex-mavenlink-965342437', 'workspaces') }} w
LEFT JOIN qbml
ON w."title" = qbml.ml_project_name