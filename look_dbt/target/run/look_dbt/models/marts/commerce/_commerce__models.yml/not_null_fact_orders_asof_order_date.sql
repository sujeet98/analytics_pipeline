
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_date
from sujeet_data_analytics_workspace.gold_dev.fact_orders_asof
where order_date is null



  
  
      
    ) dbt_internal_test