
  
  
  create or replace view sujeet_data_analytics_workspace.gold_dev.dim_distribution_center_current
  (
    `dc_sk`,
	`global_dc_id`,
	`source_system`,
	`distribution_center_id`,
	`name`,
	`latitude`,
	`longitude`,
	`valid_from`,
	`valid_to`,
	`is_current`,
	`valid_from_date`
  )
  comment 'Convenience view: current rows from dim_distribution_center (is_current = true).'
  as (
    select *
from sujeet_data_analytics_workspace.gold_dev.dim_distribution_center
where is_current = true
  )
