





with validation_errors as (

    select
        global_dc_id, valid_from
    from sujeet_data_analytics_workspace.gold_dev.dim_distribution_center
    group by global_dc_id, valid_from
    having count(*) > 1

)

select *
from validation_errors


