
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select session_gmv
from sujeet_data_analytics_workspace.gold_dev.mart_session_funnel
where session_gmv is null



  
  
      
    ) dbt_internal_test