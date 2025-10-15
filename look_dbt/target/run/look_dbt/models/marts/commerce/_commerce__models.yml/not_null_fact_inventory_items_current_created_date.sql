
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select created_date
from sujeet_data_analytics_workspace.gold_dev.fact_inventory_items_current
where created_date is null



  
  
      
    ) dbt_internal_test