
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select canonical_updated_at
from sujeet_data_analytics_workspace.silver_dev.int_commerce_users
where canonical_updated_at is null



  
  
      
    ) dbt_internal_test