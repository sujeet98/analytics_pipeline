
    
    

select
    distribution_center_id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.silver_dev.stg_look__distribution_centers
where distribution_center_id is not null
group by distribution_center_id
having count(*) > 1


