
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_day
from sujeet_data_analytics_workspace.gold_dev.time_spine
where date_day is null



  
  
      
    ) dbt_internal_test