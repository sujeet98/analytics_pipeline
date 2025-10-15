insert overwrite table sujeet_data_analytics_workspace.gold_dev.mart_session_funnel
    partition (session_date)
    select `session_id`, `session_date`, `customer_sk`, `session_start_ts`, `session_end_ts`, `traffic_source`, `f_home_event`, `f_department_event`, `f_product_event`, `f_cart_event`, `f_purchase_event`, `f_cancel_event`, `orders_count`, `session_gmv` from mart_session_funnel__dbt_tmp

