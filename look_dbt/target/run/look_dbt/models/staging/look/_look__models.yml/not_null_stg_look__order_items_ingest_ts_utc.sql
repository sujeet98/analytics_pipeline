
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ingest_ts_utc
from sujeet_data_analytics_workspace.silver_dev.stg_look__order_items
where ingest_ts_utc is null



  
  
      
    ) dbt_internal_test