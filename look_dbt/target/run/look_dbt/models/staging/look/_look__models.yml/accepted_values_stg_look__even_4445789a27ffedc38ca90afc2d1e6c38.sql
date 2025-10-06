
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.silver_dev.stg_look__events
    group by event_type

)

select *
from all_values
where value_field not in (
    'pageview','click','purchase','cancel','add_to_cart','remove_from_cart','checkout','login','signup','unknown'
)



  
  
      
    ) dbt_internal_test