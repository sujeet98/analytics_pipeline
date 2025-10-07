
    
    

with child as (
    select inventory_item_id as from_field
    from (select * from sujeet_data_analytics_workspace.silver_dev.stg_look__order_items where status in ('Complete','Shipped','Returned','Cancelled')) dbt_subquery
    where inventory_item_id is not null
),

parent as (
    select inventory_item_id as to_field
    from sujeet_data_analytics_workspace.silver_dev.stg_look__inventory_items
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


