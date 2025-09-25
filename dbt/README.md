# Look Ecommerce dbt on Databricks (Best-Practices Aligned)

This project models Bronze tables from `sujeet_data_analytics_workspace.bronze_dev`
into **staging** (atoms), **intermediate** (molecules), and **marts** (entity tables).

## Layers
- **staging/look**: 1:1 models per raw table. Renaming, typing, normalization, dedupe. No joins.
- **intermediate/commerce**: purpose-built steps (re-graining, enrichment, aggregation). **Ephemeral** by default.
- **marts/core**: entity-grained, **denormalized** tables optimized for consumption (`orders`, `order_items`, `events`),
  plus a small set of conformed dims.

## Highlights
- Unity Catalog aware (catalog = `sujeet_data_analytics_workspace`).
- Source freshness via `ingest_ts_utc`.
- Data quality with `dbt_utils` + `dbt_expectations` + custom tests.
- Incremental where it matters (`orders`, `order_items`).

## Quickstart
```bash
dbt deps
# First run
dbt build
# Typical daily run
dbt build --select marts intermediate staging
