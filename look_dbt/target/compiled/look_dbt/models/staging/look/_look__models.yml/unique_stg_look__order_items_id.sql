
    
    

select
    id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.silver_dev.stg_look__order_items
where id is not null
group by id
having count(*) > 1


