
    
    

with all_values as (

    select
        is_current as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.silver_dev.core_commerce_distribution_centers_scd
    group by is_current

)

select *
from all_values
where value_field not in (
    'True','False'
)


