
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.silver_dev.stg_look__orders
    group by status

)

select *
from all_values
where value_field not in (
    'created','processing','shipped','delivered','returned','cancelled','complete','unknown'
)


