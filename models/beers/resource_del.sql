-- resource_del.sql

with ref_resource as (
    select * from {{ ref('final_resource') }}
),

resource_del as (
    select
        resource_id,
        resource_key,
        resource_display_name,
        resource_email,
        position_role_id,
        current_date() as row_valid_from,
        row_valid_to,
        'H' as row_status,
        true as is_deleted,
        source_system
    from ref_resource
    where row_status = 'C'
      and resource_id not in (select resource_id from {{ ref('stg_resource') }})
)

select * from resource_del