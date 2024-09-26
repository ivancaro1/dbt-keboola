{{ config(
    materialized="table"
) }}

select
    id as resource_id,
    ifnull(full_name, '$N/A') as resource_display_name,
    ifnull(email_address, '$N/A') as resource_email,
    ifnull(nullif(concat(left(hex(encode("headline")), 25), right(hex(encode("headline")), 25)), ''), '-1') as position_role_id,
    account_id
from 
{{ source ('in.c-cuesta-ex-mavenlink-965342437', 'users') }}