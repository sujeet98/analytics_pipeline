
  
  
  create or replace view sujeet_data_analytics_workspace.gold_dev.dim_customer_current
  (
    `customer_sk`,
	`global_customer_id`,
	`source_system`,
	`user_id`,
	`email`,
	`first_name`,
	`last_name`,
	`gender`,
	`age`,
	`country`,
	`state`,
	`city`,
	`postal_code`,
	`traffic_source`,
	`valid_from`,
	`valid_to`,
	`is_current`,
	`valid_from_date`
  )
  comment 'Convenience view: current rows from dim_customer (is_current = true).'
  as (
    select *
from sujeet_data_analytics_workspace.gold_dev.dim_customer
where is_current = true
  )
