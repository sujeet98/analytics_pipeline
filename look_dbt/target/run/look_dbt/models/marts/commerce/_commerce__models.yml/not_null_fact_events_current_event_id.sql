
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_id
from sujeet_data_analytics_workspace.gold_dev.fact_events_current
where event_id is null



  
  
      
    ) dbt_internal_test