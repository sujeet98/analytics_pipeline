
    
    

select
    dc_sk as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.gold_dev.dim_distribution_center_current
where dc_sk is not null
group by dc_sk
having count(*) > 1


