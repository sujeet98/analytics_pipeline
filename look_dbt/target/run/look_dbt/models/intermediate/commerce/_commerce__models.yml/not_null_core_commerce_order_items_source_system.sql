
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source_system
from sujeet_data_analytics_workspace.silver_dev.core_commerce_order_items
where source_system is null



  
  
      
    ) dbt_internal_test