
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_item_id
from sujeet_data_analytics_workspace.gold_dev.fact_order_items_asof
where order_item_id is null



  
  
      
    ) dbt_internal_test