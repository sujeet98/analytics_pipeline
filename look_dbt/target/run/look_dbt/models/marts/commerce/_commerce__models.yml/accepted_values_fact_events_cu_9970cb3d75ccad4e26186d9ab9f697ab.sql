
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.gold_dev.fact_events_current
    group by event_type

)

select *
from all_values
where value_field not in (
    'cancel','purchase','product','cart','department','home','None'
)



  
  
      
    ) dbt_internal_test