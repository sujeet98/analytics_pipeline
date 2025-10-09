
    
    

with all_values as (

    select
        traffic_source as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.silver_dev.stg_look__events
    group by traffic_source

)

select *
from all_values
where value_field not in (
    'adwords','email','social','display','search','direct','referral','other'
)


