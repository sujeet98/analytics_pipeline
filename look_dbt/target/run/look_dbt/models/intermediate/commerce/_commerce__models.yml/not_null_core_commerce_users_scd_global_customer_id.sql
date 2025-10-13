
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select global_customer_id
from sujeet_data_analytics_workspace.silver_dev.core_commerce_users_scd
where global_customer_id is null



  
  
      
    ) dbt_internal_test