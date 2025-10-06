



select
    1
from sujeet_data_analytics_workspace.gold_dev.orders

where not(order_gross_revenue order_gross_revenue >= 0)

