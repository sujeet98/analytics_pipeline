
    
    

select
    inventory_item_id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.gold_dev.fact_inventory_items_asof
where inventory_item_id is not null
group by inventory_item_id
having count(*) > 1


