
    
    

with all_values as (

    select
        order_status as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.silver_dev.stg_look__orders
    group by order_status

)

select *
from all_values
where value_field not in (
    'Cancelled','Shipped','Complete','Returned','Processing'
)


