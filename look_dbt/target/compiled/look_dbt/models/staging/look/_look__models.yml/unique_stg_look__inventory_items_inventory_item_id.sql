
    
    

select
    inventory_item_id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.silver_dev.stg_look__inventory_items
where inventory_item_id is not null
group by inventory_item_id
having count(*) > 1


