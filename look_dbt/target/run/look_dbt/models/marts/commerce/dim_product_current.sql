
  
  
  create or replace view sujeet_data_analytics_workspace.gold_dev.dim_product_current
  (
    `product_sk`,
	`global_product_id`,
	`source_system`,
	`product_id`,
	`product_name`,
	`category`,
	`brand`,
	`department`,
	`sku`,
	`retail_price`,
	`distribution_center_id`,
	`valid_from`,
	`valid_to`,
	`is_current`,
	`valid_from_date`
  )
  comment 'Convenience view: current rows from dim_product (is_current = true).'
  as (
    select *
from sujeet_data_analytics_workspace.gold_dev.dim_product
where is_current = true
  )
