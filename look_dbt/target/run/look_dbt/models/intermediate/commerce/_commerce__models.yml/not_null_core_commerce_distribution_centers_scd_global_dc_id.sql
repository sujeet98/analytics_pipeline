
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select global_dc_id
from sujeet_data_analytics_workspace.silver_dev.core_commerce_distribution_centers_scd
where global_dc_id is null



  
  
      
    ) dbt_internal_test