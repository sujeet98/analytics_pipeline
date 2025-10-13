
    
    

select
    product_sk as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.silver_dev.core_commerce_products_scd
where product_sk is not null
group by product_sk
having count(*) > 1


