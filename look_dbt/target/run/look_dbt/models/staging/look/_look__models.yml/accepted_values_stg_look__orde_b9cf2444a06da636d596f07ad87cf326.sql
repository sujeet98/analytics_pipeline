
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.silver_dev.stg_look__order_items
    group by status

)

select *
from all_values
where value_field not in (
    'Complete','Shipped','Returned','Cancelled','Processing','Unknown'
)



  
  
      
    ) dbt_internal_test