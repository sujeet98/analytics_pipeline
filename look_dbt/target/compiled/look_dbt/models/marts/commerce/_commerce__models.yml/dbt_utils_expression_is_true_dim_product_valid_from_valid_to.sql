



select
    1
from sujeet_data_analytics_workspace.gold_dev.dim_product

where not(valid_from < valid_to)

