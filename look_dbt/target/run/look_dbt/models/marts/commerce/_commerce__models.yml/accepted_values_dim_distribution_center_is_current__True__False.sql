
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        is_current as value_field,
        count(*) as n_records

    from sujeet_data_analytics_workspace.gold_dev.dim_distribution_center
    group by is_current

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test