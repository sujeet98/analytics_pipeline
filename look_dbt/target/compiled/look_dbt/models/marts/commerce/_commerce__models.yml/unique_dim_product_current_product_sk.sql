
    
    

select
    product_sk as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.gold_dev.dim_product_current
where product_sk is not null
group by product_sk
having count(*) > 1


