



select
    1
from sujeet_data_analytics_workspace.silver_dev.core_commerce_distribution_centers_scd

where not(valid_from < valid_to)

