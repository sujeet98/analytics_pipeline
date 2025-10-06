
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_id
from sujeet_data_analytics_workspace.gold_dev.products_dim
where product_id is null



  
  
      
    ) dbt_internal_test