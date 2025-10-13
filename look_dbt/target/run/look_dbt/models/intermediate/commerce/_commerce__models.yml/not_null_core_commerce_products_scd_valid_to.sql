
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select valid_to
from sujeet_data_analytics_workspace.silver_dev.core_commerce_products_scd
where valid_to is null



  
  
      
    ) dbt_internal_test