
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sale_price
from sujeet_data_analytics_workspace.silver_dev.int_commerce_order_items
where sale_price is null



  
  
      
    ) dbt_internal_test