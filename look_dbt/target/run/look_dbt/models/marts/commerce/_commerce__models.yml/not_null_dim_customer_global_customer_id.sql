
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select global_customer_id
from sujeet_data_analytics_workspace.gold_dev.dim_customer
where global_customer_id is null



  
  
      
    ) dbt_internal_test