
    
    

select
    order_item_id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.gold_dev.fact_order_items_asof
where order_item_id is not null
group by order_item_id
having count(*) > 1


