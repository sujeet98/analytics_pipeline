
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.silver_dev.stg_orders
    group by status

)

select *
from all_values
where value_field not in (
    'Created','Processing','Shipped','Delivered','Returned','Cancelled','Complete'
)


