
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    distribution_center_id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.silver_dev.int_commerce_distribution_centers
where distribution_center_id is not null
group by distribution_center_id
having count(*) > 1



  
  
      
    ) dbt_internal_test