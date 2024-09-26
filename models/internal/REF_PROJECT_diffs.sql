{{ config(
    materialized='table'
) }}

select
	project_key
  , project_id
  , project_name
  , project_start_date
  , project_status_id
  , service_line_id
  , row_valid_from
  , row_valid_to
  , row_status
  , is_deleted
  , source_system
from {{ ref('REF_PROJECT_ref_proyect_new') }}

except

select
	project_key
  , project_id
  , project_name
  , project_start_date
  , project_status_id
  , service_line_id
  , row_valid_from
  , row_valid_to
  , row_status
  , is_deleted
  , source_system
from {{ source('out.c-MODEL', 'REF_PROJECT') }}
where 1 = 1
	and row_status = 'C'