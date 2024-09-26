{{ config(
    materialized="table"
) }}
SELECT
  "STATS_ID"            AS stats_id_dbt,
  TRIM("NAME")          AS name_dbt,
  "ROW_COUNTS"         AS row_counts_dbt,
  "COUNT_DATE"           AS count_date_dbt,
  "timestamp"           AS timestamp_dbt,
FROM
{{ source ('in.c-test', 'test') }}