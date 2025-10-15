
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        order_status as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.gold_dev.fact_orders_asof
    group by order_status

)

select *
from all_values
where value_field not in (
    'Cancelled','Shipped','Complete','Returned','Processing','None'
)



  
  
      
    ) dbt_internal_test