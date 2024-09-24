
{{ config(
    materialized="table"
) }}

SELECT
  "ID"            AS beer_id,
  TRIM("NAME")    AS beer_name,
  "STYLE"         AS beer_style,
  "ABV"           AS abv,
  "IBU"           AS ibu,
  CASE 
       WHEN "IBU" <= 50 THEN 'Malty'
       WHEN "IBU" > 50 THEN 'Hoppy'
   END AS bitterness,
  "BREWERY_ID"    AS brewery_id,
  "OUNCES"        AS ounces
FROM
  {{ source('WORKSPACE_1183594922', 'seed_beers') }}
