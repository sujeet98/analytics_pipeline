# Delivery Checklist — TheLook Commerce Analytics

Legend: ✅ Done · 🟨 In Progress/Partial · ⛔ To Do

_Last updated: now_

---

## 1) Foundations & Access
- ✅ **Repo structure & env files** present; `.gitignore` includes envs; `venv/` should be removed from git history if still tracked.
- ✅ **Databricks + Unity Catalog** available; External Volume for RAW configured.
- ✅ **dbt project** set up (`dbt_project.yml`, profiles).

**Considerations:** Remove any committed `venv/`. Pin deps & add `requirements.lock.txt` later.

---

## 2) Source & Contracts
- ✅ **Source identified**: BigQuery Public TheLook.
- ✅ **Access method**: Spark BQ connector; config supports ADC/Secrets.
- 🟨 **Late-arrival policy**: grace window implemented in ingestion; document final SLA in PRD as 30–60 min.
- ⛔ **Formal data contract** (PK, loaded_at, schema evolution) not signed w/ source (public dataset).

**Next:** Keep grace window >= the largest observed delay (recommend 2–7 days buffer for merges).

---

## 3) Ingestion (BigQuery → RAW files)
- ✅ **Incremental ingestion script** `ingest_bigquery_to_raw.py` with state & grace window.
- ✅ **RAW layout**: partitioned by `ingest_date`/`run_ts`; system cols enforced.
- 🟨 **State store**: JSON files; recommend migrating to a tiny Delta table for observability.

**Next:** Add retry/backoff around BQ reads; write audit rows (counts, window bounds) to an ingest_audit Delta table.

---

## 4) RAW → Bronze (Auto Loader)
- ✅ **Auto Loader script** `raw_to_bronze_autoloader.py` with checkpoints/schemaLocation.
- ✅ **Bronze tables** created in UC.
- 🟨 **rescuedDataColumn** / notification mode not explicitly set.

**Next:** Enable `cloudFiles.rescuedDataColumn` and (if possible) bucket notifications for faster discovery.

---

## 5) Silver (Staging)
- ✅ **Deduping & normalization** per table; `src_ingest_ts` derived.
- ✅ **Materialization**: switched to **incremental MERGE** per model.
- ✅ **Tests**: PK uniqueness, enums, ranges (as defined in YAML).

**Next:** Ensure all staging models include `unique_key` and incremental predicate; add FK tests where helpful.

---

## 6) Intermediate
- ✅ **`int_order_items_enriched`** heavy join set to **incremental MERGE**.
- ✅ **`int_orders_aggregated_from_items`** as **table** (simple aggregation).
- 🟨 **Reusability** documented; usage by marts confirmed.

**Next:** If intermediate grows, consider more granular models (e.g., inventory state, net revenue).

---

## 7) Gold (Marts)
- ✅ **Facts (`orders`, `order_items`)** converted to **incremental MERGE** with pruning.
- 🟨 **`events`**: incremental strategy chosen (append or merge) — confirm final choice.
- ✅ **Dims (`products_dim`, `users_dim`, `distribution_centers_dim`)** exist as tables.

**Next:** Cast metric outputs to DECIMAL(18,2) consistently; add additional business metrics as needed.

---

## 8) Snapshots (SCD2)
- 🟨 **Snapshot files** (`users_scd`, `products_scd`) templated & PRD documented.
- ⛔ **Snapshots executed/scheduled**; gold dims reading from `_scd where dbt_valid_to is null` only partially applied.

**Next:** Run `dbt snapshot`, switch `users_dim`/`products_dim` to snapshot sources, and add snapshot cadence to orchestration.

---

## 9) Data Quality & Freshness
- ✅ **Model tests** across layers (PK/uniqueness, enums, relationships, ranges).
- 🟨 **Source freshness**: configured; thresholds set (warn 3h / error 6h).
- ⛔ **Automated reconciliation**: counts across Bronze→Silver→Gold not yet logged per run.

**Next:** Add a small dbt audit model that logs row deltas & freshness into `monitoring.run_audit` table.

---

## 10) Orchestration
- 🟨 **Manual runbook** established; end-to-end commands documented.
- ⛔ **Databricks Workflow** (3 tasks) not yet created.
- ⛔ **CI (GitHub Actions)** slim‑CI not enabled.

**Next:** Create Workflow: (1) BQ→RAW, (2) RAW→Bronze, (3) dbt snapshot/run/test/freshness. Add GH Action for PR builds (`state:modified+`).

---

## 11) Observability & Ops
- ⛔ **Run metrics table** (rows, bounds, duration, max lag) not yet implemented.
- ⛔ **Alerts**: on failures, test errors, freshness breaches not wired.
- 🟨 **Maintenance**: OPTIMIZE/ZORDER/VACUUM recommended; schedule pending.

**Next:** Weekly OPTIMIZE/ZORDER for gold facts; set `delta.autoOptimize` table properties; add Slack/email alerts.

---

## 12) Security & Governance
- 🟨 **UC permissions** by schema assumed; PII masking policy optional.
- ⛔ **Tags/metadata** for PII columns; column masking not applied.

**Next:** Tag PII in `users_dim`; optionally add a masking policy to `email` for non‑PII roles.

---

## 13) Docs & Exposure
- 🟨 **persist_docs** enabled; dbt docs can be built.
- ⛔ **Exposures** linking dashboards to gold models not yet added.
- ⛔ **Architecture diagram** and runbook screenshots not yet in README.

**Next:** Add `exposures.yml`; include DAG & system diagram in repo README.

---

## 14) Sign‑off & SLA
- 🟨 **SLA stated**: 30–60 min; needs dashboard freshness display & alert.
- ⛔ **UAT sign‑off**: run acceptance queries with “business” stakeholder.

**Next:** Capture UAT sign‑off notes and publish the SLA in README.

---

## Concrete Next Actions (prioritized)
1. **Create Databricks Workflow** (3 tasks) + schedule every 30 min.  
2. **Run `dbt snapshot` and switch dims** to SCD sources; add snapshot step to the Workflow.  
3. **Add audit models** to log freshness & row deltas; wire alerts for failures/freshness.  
4. **Enable Delta maintenance**: set autoOptimize, schedule weekly OPTIMIZE/ZORDER & VACUUM.  
5. **Decide events strategy** (append vs merge) and lock it.  
6. **Add exposures.yml** and a minimal Sales & Events dashboard (even mock) for lineage.  
7. (Optional) **Migrate ingest state** to a Delta table; add retry/backoff.  
8. **CI (GitHub Actions)**: slim‑CI (`state:modified+`) with test & docs artifact.

---

### Appendix — Quick sanity SQL
- Bronze row counts by table (compare across layers)
- Freshness checks: `max(src_ingest_ts)` vs current time
- FK integrity checks in Gold (e.g., items → products_dim)