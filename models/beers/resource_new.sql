-- resource_new.sql
{{ config(
    materialized="table"
) }}
with ref_resource as (
    select * from {{ ref('final_resource') }}
),

resource_new as (
    select 
        resource_id,
        resource_key,
        resource_display_name,
        resource_email,
        position_role_id,
        row_valid_to,
        row_status,
        is_deleted,
        source_system
    from {{ ref('stg_resource') }}
    except
    select
        resource_id,
        resource_key,
        resource_display_name,
        resource_email,
        position_role_id,
        row_valid_to,
        row_status,
        is_deleted,
        source_system
    from ref_resource
    where row_status = 'C'
)

select
    resource_id,
    resource_key,
    resource_display_name,
    resource_email,
    position_role_id,
    current_date() as row_valid_from,
    row_valid_to,
    row_status,
    is_deleted,
    source_system
from resource_new