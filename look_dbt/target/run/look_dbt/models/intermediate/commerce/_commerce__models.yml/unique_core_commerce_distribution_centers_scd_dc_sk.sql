
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    dc_sk as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.silver_dev.core_commerce_distribution_centers_scd
where dc_sk is not null
group by dc_sk
having count(*) > 1



  
  
      
    ) dbt_internal_test