
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select created_date
from sujeet_data_analytics_workspace.silver_dev.int_commerce_inventory_items
where created_date is null



  
  
      
    ) dbt_internal_test