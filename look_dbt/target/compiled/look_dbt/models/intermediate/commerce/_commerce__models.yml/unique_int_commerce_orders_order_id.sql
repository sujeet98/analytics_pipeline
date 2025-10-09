
    
    

select
    order_id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.silver_dev.int_commerce_orders
where order_id is not null
group by order_id
having count(*) > 1


