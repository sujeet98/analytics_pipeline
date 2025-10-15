
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select orders_count
from sujeet_data_analytics_workspace.gold_dev.mart_session_funnel
where orders_count is null



  
  
      
    ) dbt_internal_test