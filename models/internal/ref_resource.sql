{{ config(
    materialized="table"
) }}

select distinct
    ifnull(nullif("Id", ''), '-1') as customer_id
    , ifnull(nullif(concat(left(hex_encode("Type")::varchar,10), right(hex_encode("Type")::varchar,10)),''),'-1') as customer_type_id
    , '-1' as customer_status_id
    , ifnull(nullif("Industry", ''), '-1') as industry_type_id
    , concat('SALESFORCE | ',"Id") as customer_key
    , ifnull(nullif("Name", ''), '$N/A') as customer_name
    , nullif("BillingStreet", '') as customer_address_line_1
    , nullif("BillingState", '') as customer_address_line_2
    , nullif("BillingPostalCode", '') as customer_postal_code
    , nullif("BillingCity", '') as customer_city
    , to_date(left(nullif("LastModifiedDate", ''),10), 'yyyy-mm-dd') as row_valid_from
    , to_date('9999-12-31', 'yyyy-mm-dd') as row_valid_to
    , 'C' as row_status
    , "IsDeleted" as is_deleted
    , 'SALESFORCE' as source_system
from 
{{ source ('in.c-kds-team-ex-salesforce-v2-1053666815', 'Account') }}