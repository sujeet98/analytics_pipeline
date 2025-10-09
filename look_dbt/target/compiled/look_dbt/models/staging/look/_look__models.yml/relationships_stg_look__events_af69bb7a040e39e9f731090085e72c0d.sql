
    
    

with child as (
    select user_id as from_field
    from (select * from sujeet_data_analytics_workspace.silver_dev.stg_look__events where user_id is not null) dbt_subquery
    where user_id is not null
),

parent as (
    select id as to_field
    from sujeet_data_analytics_workspace.silver_dev.stg_look__users
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


