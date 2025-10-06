
    
    

select
    product_id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.silver_dev.stg_look__products
where product_id is not null
group by product_id
having count(*) > 1


