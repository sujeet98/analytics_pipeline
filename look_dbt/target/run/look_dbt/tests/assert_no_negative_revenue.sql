
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select *
from sujeet_data_analytics_workspace.gold_dev.orders
where order_gross_revenue < 0
  
  
      
    ) dbt_internal_test