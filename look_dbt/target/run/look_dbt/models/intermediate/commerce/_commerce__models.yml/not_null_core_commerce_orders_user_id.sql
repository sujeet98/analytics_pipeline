
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select user_id
from sujeet_data_analytics_workspace.silver_dev.core_commerce_orders
where user_id is null



  
  
      
    ) dbt_internal_test