
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unit_cost
from sujeet_data_analytics_workspace.silver_dev.stg_look__products
where unit_cost is null



  
  
      
    ) dbt_internal_test