
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_sk
from sujeet_data_analytics_workspace.silver_dev.core_commerce_users_scd
where customer_sk is null



  
  
      
    ) dbt_internal_test