
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select name
from sujeet_data_analytics_workspace.silver_dev.int_commerce_distribution_centers
where name is null



  
  
      
    ) dbt_internal_test