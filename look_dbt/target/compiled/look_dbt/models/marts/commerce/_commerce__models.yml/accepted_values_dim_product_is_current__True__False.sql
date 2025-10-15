
    
    

with all_values as (

    select
        is_current as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.gold_dev.dim_product
    group by is_current

)

select *
from all_values
where value_field not in (
    'True','False'
)


