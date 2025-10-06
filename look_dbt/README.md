# look_dbt

## Layers
- **staging (silver_dev)**: 1:1 with sources, no joins, views.
- **intermediate (ephemeral)**: purpose-specific (re-graining, pre-joins), not user-facing.
- **marts/core (gold_dev)**: wide, denormalized entities; tables/incremental with constraints.

## Commands
- Build staging (Look): `dbt build --selector staging_look`
- Build marts with parents: `dbt build --selector marts_core_with_ancestors`
- Only changed + parents: `dbt build --selector changed_plus_parents`

## Quality
- Tests live in folder YAML, under `arguments:` (dbt v2 syntax).
- Referential gaps kept as failing tests to surface source issues.
- Elementary models (first time): `dbt run -s elementary --target dev`

## Conventions
- File names: `base_...`, `stg_...`, `int_..._verb`, marts named by entity.
- Constraints on marts: primary keys + not null.
- Docs & exposures maintained in YAML; run `dbt docs generate && dbt docs serve`.
