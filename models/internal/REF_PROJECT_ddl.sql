{{ config(
    materialized='table'  
) }}

WITH data AS (
  SELECT 
    'MAVENLINK | -1' AS project_key,
    '-1' AS project_id,
    '$N/A' AS project_name,
    NULL AS project_start_date,
    '-1' AS project_status_id,
    '-1' AS service_line_id,
    TO_DATE('2000-01-01', 'YYYY-MM-DD') AS row_valid_from,
    TO_DATE('9999-12-31', 'YYYY-MM-DD') AS row_valid_to,
    'C' AS row_status,
    False AS is_deleted,
    'MAVENLINK' AS source_system
)

SELECT * FROM data
