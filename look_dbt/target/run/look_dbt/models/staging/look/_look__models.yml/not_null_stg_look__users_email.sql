
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select email
from sujeet_data_analytics_workspace.silver_dev.stg_look__users
where email is null



  
  
      
    ) dbt_internal_test