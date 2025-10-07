
    
    

with child as (
    select product_id as from_field
    from sujeet_data_analytics_workspace.silver_dev.int_order_items_enriched
    where product_id is not null
),

parent as (
    select product_id as to_field
    from sujeet_data_analytics_workspace.silver_dev.stg_look__products
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


