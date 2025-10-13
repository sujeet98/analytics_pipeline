
    
    

select
    global_order_item_id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.silver_dev.core_commerce_order_items
where global_order_item_id is not null
group by global_order_item_id
having count(*) > 1


