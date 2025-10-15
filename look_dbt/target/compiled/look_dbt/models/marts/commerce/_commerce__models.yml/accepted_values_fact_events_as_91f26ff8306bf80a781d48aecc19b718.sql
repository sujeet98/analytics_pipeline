
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.gold_dev.fact_events_asof
    group by event_type

)

select *
from all_values
where value_field not in (
    'cancel','purchase','product','cart','department','home','None'
)


