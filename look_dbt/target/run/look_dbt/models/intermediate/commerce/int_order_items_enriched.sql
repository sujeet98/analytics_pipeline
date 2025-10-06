-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        sujeet_data_analytics_workspace.silver_dev.int_order_items_enriched as DBT_INTERNAL_DEST
    using
        int_order_items_enriched__dbt_tmp as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.order_item_id <=> DBT_INTERNAL_DEST.order_item_id
          
    when matched
        then update set
            `order_item_id` = DBT_INTERNAL_SOURCE.`order_item_id`, `order_id` = DBT_INTERNAL_SOURCE.`order_id`, `user_id` = DBT_INTERNAL_SOURCE.`user_id`, `product_id` = DBT_INTERNAL_SOURCE.`product_id`, `inventory_item_id` = DBT_INTERNAL_SOURCE.`inventory_item_id`, `item_status` = DBT_INTERNAL_SOURCE.`item_status`, `sale_price` = DBT_INTERNAL_SOURCE.`sale_price`, `item_created_at` = DBT_INTERNAL_SOURCE.`item_created_at`, `inv_product_id` = DBT_INTERNAL_SOURCE.`inv_product_id`, `product_distribution_center_id` = DBT_INTERNAL_SOURCE.`product_distribution_center_id`, `product_name` = DBT_INTERNAL_SOURCE.`product_name`, `product_brand` = DBT_INTERNAL_SOURCE.`product_brand`, `product_category` = DBT_INTERNAL_SOURCE.`product_category`, `product_department` = DBT_INTERNAL_SOURCE.`product_department`, `retail_price` = DBT_INTERNAL_SOURCE.`retail_price`, `product_sku` = DBT_INTERNAL_SOURCE.`product_sku`, `distribution_center_id` = DBT_INTERNAL_SOURCE.`distribution_center_id`, `distribution_center_name` = DBT_INTERNAL_SOURCE.`distribution_center_name`, `distribution_center_latitude` = DBT_INTERNAL_SOURCE.`distribution_center_latitude`, `distribution_center_longitude` = DBT_INTERNAL_SOURCE.`distribution_center_longitude`
    when not matched
        then insert
            (`order_item_id`, `order_id`, `user_id`, `product_id`, `inventory_item_id`, `item_status`, `sale_price`, `item_created_at`, `inv_product_id`, `product_distribution_center_id`, `product_name`, `product_brand`, `product_category`, `product_department`, `retail_price`, `product_sku`, `distribution_center_id`, `distribution_center_name`, `distribution_center_latitude`, `distribution_center_longitude`) VALUES (DBT_INTERNAL_SOURCE.`order_item_id`, DBT_INTERNAL_SOURCE.`order_id`, DBT_INTERNAL_SOURCE.`user_id`, DBT_INTERNAL_SOURCE.`product_id`, DBT_INTERNAL_SOURCE.`inventory_item_id`, DBT_INTERNAL_SOURCE.`item_status`, DBT_INTERNAL_SOURCE.`sale_price`, DBT_INTERNAL_SOURCE.`item_created_at`, DBT_INTERNAL_SOURCE.`inv_product_id`, DBT_INTERNAL_SOURCE.`product_distribution_center_id`, DBT_INTERNAL_SOURCE.`product_name`, DBT_INTERNAL_SOURCE.`product_brand`, DBT_INTERNAL_SOURCE.`product_category`, DBT_INTERNAL_SOURCE.`product_department`, DBT_INTERNAL_SOURCE.`retail_price`, DBT_INTERNAL_SOURCE.`product_sku`, DBT_INTERNAL_SOURCE.`distribution_center_id`, DBT_INTERNAL_SOURCE.`distribution_center_name`, DBT_INTERNAL_SOURCE.`distribution_center_latitude`, DBT_INTERNAL_SOURCE.`distribution_center_longitude`)

