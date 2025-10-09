
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        traffic_source as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.silver_dev.int_commerce_events
    group by traffic_source

)

select *
from all_values
where value_field not in (
    'Adwords','Organic','Email','Facebook','YouTube'
)



  
  
      
    ) dbt_internal_test