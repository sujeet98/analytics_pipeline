
    
    

with child as (
    select distribution_center_id as from_field
    from sujeet_data_analytics_workspace.silver_dev.int_order_items_enriched
    where distribution_center_id is not null
),

parent as (
    select distribution_center_id as to_field
    from sujeet_data_analytics_workspace.silver_dev.stg_look__distribution_centers
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


