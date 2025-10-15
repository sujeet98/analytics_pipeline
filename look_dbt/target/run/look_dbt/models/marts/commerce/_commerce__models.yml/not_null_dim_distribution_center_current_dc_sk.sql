
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dc_sk
from sujeet_data_analytics_workspace.gold_dev.dim_distribution_center_current
where dc_sk is null



  
  
      
    ) dbt_internal_test