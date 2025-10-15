
    
    

with all_values as (

    select
        traffic_source as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.gold_dev.fact_events_asof
    group by traffic_source

)

select *
from all_values
where value_field not in (
    'Adwords','Organic','Email','Facebook','YouTube','None'
)


