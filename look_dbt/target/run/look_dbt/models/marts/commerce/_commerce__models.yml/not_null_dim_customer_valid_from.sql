
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select valid_from
from sujeet_data_analytics_workspace.gold_dev.dim_customer
where valid_from is null



  
  
      
    ) dbt_internal_test