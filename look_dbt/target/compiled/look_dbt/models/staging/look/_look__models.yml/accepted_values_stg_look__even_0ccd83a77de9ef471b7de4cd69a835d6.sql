
    
    

with all_values as (

    select
        browser as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.silver_dev.stg_look__events
    group by browser

)

select *
from all_values
where value_field not in (
    'Firefox','Other','IE','Safari','Chrome'
)


