
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        item_status as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.silver_dev.stg_look__order_items
    group by item_status

)

select *
from all_values
where value_field not in (
    'complete','processing','returned','shipped','None'
)



  
  
      
    ) dbt_internal_test