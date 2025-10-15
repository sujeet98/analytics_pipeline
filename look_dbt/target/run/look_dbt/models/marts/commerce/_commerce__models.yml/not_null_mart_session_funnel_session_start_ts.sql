
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select session_start_ts
from sujeet_data_analytics_workspace.gold_dev.mart_session_funnel
where session_start_ts is null



  
  
      
    ) dbt_internal_test