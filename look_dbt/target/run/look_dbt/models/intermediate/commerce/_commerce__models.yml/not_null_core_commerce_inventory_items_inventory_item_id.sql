
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select inventory_item_id
from sujeet_data_analytics_workspace.silver_dev.core_commerce_inventory_items
where inventory_item_id is null



  
  
      
    ) dbt_internal_test