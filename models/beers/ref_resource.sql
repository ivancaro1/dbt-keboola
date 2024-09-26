-- ref_resource.sql
{{ config(
    materialized="table"
) }}with ref_resource as (
    select * from {{ ref('your_source_table') }}
),

resource_update as (
    select
        r.resource_id,
        r.resource_key,
        r.resource_display_name,
        r.resource_email,
        r.position_role_id,
        rn.row_valid_from,
        r.row_valid_to,
        'H' as row_status,
        r.is_deleted,
        r.source_system
    from ref_resource r
    inner join {{ ref('resource_new') }} rn
    on r.resource_id = rn.resource_id
    where r.row_status = 'C'
),

resource_insert as (
    select
        resource_id,
        resource_key,
        resource_display_name,
        resource_email,
        position_role_id,
        row_valid_from,
        row_valid_to,
        row_status,
        is_deleted,
        source_system
    from {{ ref('resource_new') }}
    union all
    select
        resource_id,
        resource_key,
        resource_display_name,
        resource_email,
        position_role_id,
        row_valid_from,
        row_valid_to,
        row_status,
        is_deleted,
        source_system
    from {{ ref('resource_del') }}
)

select * from resource_update
union all
select * from resource_insert