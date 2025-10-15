
    
    

select
    session_id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.gold_dev.mart_session_funnel
where session_id is not null
group by session_id
having count(*) > 1


