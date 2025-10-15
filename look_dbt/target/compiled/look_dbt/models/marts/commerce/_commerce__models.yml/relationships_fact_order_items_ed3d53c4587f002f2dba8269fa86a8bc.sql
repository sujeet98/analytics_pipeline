
    
    

with child as (
    select product_sk as from_field
    from sujeet_data_analytics_workspace.gold_dev.fact_order_items_current
    where product_sk is not null
),

parent as (
    select product_sk as to_field
    from sujeet_data_analytics_workspace.gold_dev.dim_product
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


