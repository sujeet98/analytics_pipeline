
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    session_id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.gold_dev.mart_session_funnel
where session_id is not null
group by session_id
having count(*) > 1



  
  
      
    ) dbt_internal_test