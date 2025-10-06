
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_id
from sujeet_data_analytics_workspace.gold_dev.events
where event_id is null



  
  
      
    ) dbt_internal_test