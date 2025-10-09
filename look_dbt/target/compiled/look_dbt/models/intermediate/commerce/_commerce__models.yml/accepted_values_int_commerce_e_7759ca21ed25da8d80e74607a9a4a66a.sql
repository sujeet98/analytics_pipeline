
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.silver_dev.int_commerce_events
    group by event_type

)

select *
from all_values
where value_field not in (
    'cancel','purchase','product','cart','department','home'
)


