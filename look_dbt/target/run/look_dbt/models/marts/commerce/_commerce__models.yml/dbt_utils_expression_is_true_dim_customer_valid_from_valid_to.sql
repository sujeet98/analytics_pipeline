
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from sujeet_data_analytics_workspace.gold_dev.dim_customer

where not(valid_from < valid_to)


  
  
      
    ) dbt_internal_test