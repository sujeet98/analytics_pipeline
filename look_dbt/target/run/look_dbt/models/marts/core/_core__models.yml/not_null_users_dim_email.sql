
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select email
from sujeet_data_analytics_workspace.gold_dev.users_dim
where email is null



  
  
      
    ) dbt_internal_test