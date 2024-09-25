
{{ config(
    materialized="table"
) }}

SELECT
  "ID"                    AS brewery_id,
  trim("NAME")            AS brewery_name,
  trim("CITY")            AS brewery_city,
  trim("STATE")           AS brewery_state,
  'USA'                 AS brewery_country
FROM
{{ ref('seed_breweries') }}