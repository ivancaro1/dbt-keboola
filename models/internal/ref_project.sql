{{ config(materialized="table") }}

-- create what the current view should look like
with ref_project_new as (
    select 
        CONCAT('MAVENLINK | ', "id") AS project_key,
        ifnull(nullif("id", ''), '-1') AS project_id,
        ifnull(nullif("title", ''), '$N/A') AS project_name,
        nullif("start_date", '') AS project_start_date,
        ifnull(nullif("status_key", '')::INT, '-1') AS project_status_id,
        ifnull(nullif(concat(left(hex_encode(qbml.service_line)::varchar, 10), right(hex_encode(qbml.service_line)::varchar, 10)), ''), '-1') AS service_line_id,
        try_to_date('updated_at') AS row_valid_from,
        to_date('9999-12-31', 'YYYY-MM-DD') AS row_valid_to,
        'C' AS row_status,
        false AS is_deleted,
        'MAVENLINK' AS source_system
    from {{ source ('in.c-test','workspaces') }} w
    left join (
        select distinct ml_project_name, service_line 
        from {{ source ('in.c-test','REF_QB_ML_PROJECT_SL_MAPPING') }}
    ) qbml
    on w."title" = qbml.ml_project_name
)

-- create the diffs
, diffs as (
    select 
        project_key,
        project_id,
        project_name,
        project_start_date,
        project_status_id,
        service_line_id,
        row_valid_from,
        row_valid_to,
        row_status,
        is_deleted,
        source_system
    from ref_project_new
    
    except 
    
    select 
        project_key,
        project_id,
        project_name,
        project_start_date,
        project_status_id,
        service_line_id,
        row_valid_from,
        row_valid_to,
        row_status,
        is_deleted,
        source_system
    from {{ source ( 'out.c-MODEL','REF_PROJECT' )}}
    where row_status = 'C'
)

-- only keep records without diffs
, ref_project_no_diffs as (
    select 
        rp.project_key,
        rp.project_id,
        rp.project_name,
        rp.project_start_date,
        rp.project_status_id,
        rp.service_line_id,
        rp.row_valid_from,
        rp.row_valid_to,
        rp.row_status,
        rp.is_deleted,
        rp.source_system
    from {{ source ( 'out.c-MODEL','REF_PROJECT' )}} rp
    left join diffs d on d.project_id = rp.project_id
    where d.project_id is null
)

-- combine all records
, ref_project_all_records as (
    select * from ref_project_no_diffs

    union all 

    select 
        project_key,
        project_id,
        project_name,
        project_start_date,
        project_status_id,
        service_line_id,
        row_valid_from,
        row_valid_to,
        row_status,
        is_deleted,
        source_system
    from diffs
)

-- final output
select 
    rpar.project_key,
    rpar.project_id,
    rpar.project_name,
    rpar.project_start_date,
    rpar.project_status_id,
    rpar.service_line_id,
    rpar.row_valid_from,
    rpar.row_valid_to,
    iff(rpn.project_id is null, 1, 0) as is_deleted,
    rpar.source_system
from ref_project_all_records rpar
left join ref_project_new rpn on rpn.project_id = rpar.project_id