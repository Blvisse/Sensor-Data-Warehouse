with source as (
    select * from {{ ref('station_summary') }}
),

stage_summary as

(

select 
    ID,
    flow_99,
    flow_max,
    flow_median,
    flow_total,
    sqly 

from source
)

select * from stage_summary