
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select created_at
from sujeet_data_analytics_workspace.silver_dev.stg_look__events
where created_at is null



  
  
      
    ) dbt_internal_test