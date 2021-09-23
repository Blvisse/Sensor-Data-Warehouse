with median as (

    select * 

    from {{ ref ('stg_80_median') }}


),

station as (
    select *  from {{ ref ('stg_station') }}

),

summary as (
    select * from {{ ref ('stg_summary') }}

),

final as (
    
    select
    median.ID,
    station.ID,

    


    
    
    
    )

