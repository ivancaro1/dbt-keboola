
-- create what the current view should look like
with ref_project_status_new as (
    select
        distinct ifnull("status_key", '-1')::varchar(50) as project_status_id
        , concat('MAVENLINK | ', project_status_id)::varchar(50) as project_status_key
        , ifnull(nullif("status_message",''), '$N/A')::varchar(50) as project_status_name
        , False::boolean as is_deleted
        , 'MAVENLINK'::varchar(30) as source_system
    from {{ source ('in.c-cuesta-ex-mavenlink-965342437','workspaces') }}
)

-- create the diffs
, status_new as (
    select
    	project_status_id
      , project_status_key
      , project_status_name
      , is_deleted
      , source_system
    from ref_project_status_new
    
    except 
    
    select
    	project_status_id
      , project_status_key
      , project_status_name
      , is_deleted
      , source_system
    
    from {{ source ( 'out.c-MODEL','REF_PROJECT_STATUS' )}}
)

-- only want records that don't have diffs
, ref_project_status_no_diffs as (
    select
      ps.project_status_id
      , ps.project_status_key
      , ps.project_status_name
      , ps.is_deleted
      , ps.source_system
    from {{ source ( 'out.c-MODEL','REF_PROJECT_STATUS' )}} ps
    left join status_new sn on sn.project_status_id = ps.project_status_id
    where 1=1
      and sn.project_status_id is null
)


, ref_project_status_all_records as (
select
*
from ref_project_status_no_diffs

union 

select
	project_status_id
  , project_status_key
  , project_status_name
  , is_deleted
  , source_system
from status_new
)

select
  psar.project_status_id
  , psar.project_status_key
  , psar.project_status_name
  , iff(psn.project_status_id is null, 1, 0) as is_deleted
  , psar.source_system
from ref_project_status_all_records psar
left join ref_project_status_new psn on psn.project_status_id = psar.project_status_id
