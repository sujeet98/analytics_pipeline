





with validation_errors as (

    select
        global_product_id, valid_from
    from sujeet_data_analytics_workspace.gold_dev.dim_product
    group by global_product_id, valid_from
    having count(*) > 1

)

select *
from validation_errors


