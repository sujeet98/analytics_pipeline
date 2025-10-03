# Look Ecommerce dbt on Databricks (Best-Practices Aligned)

This project models Bronze tables from `sujeet_data_analytics_workspace.bronze_dev` into:

- **staging** (atoms) — 1:1 cleaned views of raw tables (renaming, typing, dedupe, no joins)
- **intermediate** (molecules) — purpose-built steps (re-graining, enrichment)
- **marts** (entities) — denormalized, entity-grained tables optimized for analytics

## Quickstart

```bash
# 1) install packages listed in packages.yml
dbt deps

# 2) first run builds all layers and tests
dbt build

# 3) typical daily run — build in dependency order
dbt build --select marts intermediate staging
