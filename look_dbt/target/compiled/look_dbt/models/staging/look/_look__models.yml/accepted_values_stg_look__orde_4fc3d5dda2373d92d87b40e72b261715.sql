
    
    

with all_values as (

    select
        item_status as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.silver_dev.stg_look__order_items
    group by item_status

)

select *
from all_values
where value_field not in (
    'complete','processing','returned','shipped','None'
)


