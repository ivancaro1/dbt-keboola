-- stg_resource.sql

with users as (
    select
        id as resource_id,
        ifnull(full_name, '$N/A') as resource_display_name,
        ifnull(email_address, '$N/A') as resource_email,
        ifnull(nullif(concat(left(hex(encode("headline")), 25), right(hex(encode("headline")), 25)), ''), '-1') as position_role_id,
        account_id
    from {{ source ('in.c-cuesta-ex-mavenlink-965342437', 'users') }}
)

select
    ifnull(resource_id, '-1') as resource_id,
    concat('MAVENLINK | ', resource_id) as resource_key,
    resource_display_name,
    resource_email,
    position_role_id,
    to_date('2019-01-01') as row_valid_from,
    to_date('9999-12-31') as row_valid_to,
    'C' as row_status,
    false as is_deleted,
    'MAVENLINK' as source_system
from users
where account_id = '7517015'