
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_sk
from sujeet_data_analytics_workspace.gold_dev.fact_order_items_asof
where customer_sk is null



  
  
      
    ) dbt_internal_test