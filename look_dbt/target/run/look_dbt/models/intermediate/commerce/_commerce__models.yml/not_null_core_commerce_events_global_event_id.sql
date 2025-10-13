
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select global_event_id
from sujeet_data_analytics_workspace.silver_dev.core_commerce_events
where global_event_id is null



  
  
      
    ) dbt_internal_test