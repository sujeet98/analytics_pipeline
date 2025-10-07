

-- Wide order item fact with product + DC context.
-- Incremental pruning via src_ingest_ts (from the order item).

select * from sujeet_data_analytics_workspace.silver_dev.int_order_items_enriched
