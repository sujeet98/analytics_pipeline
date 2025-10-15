
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select global_product_id
from sujeet_data_analytics_workspace.gold_dev.dim_product
where global_product_id is null



  
  
      
    ) dbt_internal_test