
    
    

with child as (
    select product_distribution_center_id as from_field
    from (select * from sujeet_data_analytics_workspace.silver_dev.stg_look__inventory_items where product_distribution_center_id is not null) dbt_subquery
    where product_distribution_center_id is not null
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


