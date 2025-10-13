





with validation_errors as (

    select
        global_product_id, valid_from
    from sujeet_data_analytics_workspace.silver_dev.core_commerce_products_scd
    group by global_product_id, valid_from
    having count(*) > 1

)

select *
from validation_errors


