



select
    1
from sujeet_data_analytics_workspace.silver_dev.core_commerce_users_scd

where not(valid_from < valid_to)

