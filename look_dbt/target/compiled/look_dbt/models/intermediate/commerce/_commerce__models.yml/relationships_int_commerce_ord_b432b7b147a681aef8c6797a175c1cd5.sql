
    
    

with child as (
    select inventory_item_id as from_field
    from (select * from sujeet_data_analytics_workspace.silver_dev.int_commerce_order_items where inventory_item_id is not null) dbt_subquery
    where inventory_item_id is not null
),

parent as (
    select inventory_item_id as to_field
    from sujeet_data_analytics_workspace.silver_dev.int_commerce_inventory_items
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


