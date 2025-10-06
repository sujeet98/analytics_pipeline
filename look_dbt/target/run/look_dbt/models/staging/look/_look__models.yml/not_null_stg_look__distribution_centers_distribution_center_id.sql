
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select distribution_center_id
from sujeet_data_analytics_workspace.default_silver_dev.stg_look__distribution_centers
where distribution_center_id is null



  
  
      
    ) dbt_internal_test