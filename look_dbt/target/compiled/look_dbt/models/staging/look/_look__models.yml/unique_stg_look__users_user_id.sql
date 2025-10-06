
    
    

select
    user_id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.silver_dev.stg_look__users
where user_id is not null
group by user_id
having count(*) > 1


