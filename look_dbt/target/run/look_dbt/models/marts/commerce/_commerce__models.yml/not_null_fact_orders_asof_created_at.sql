
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select created_at
from sujeet_data_analytics_workspace.gold_dev.fact_orders_asof
where created_at is null



  
  
      
    ) dbt_internal_test