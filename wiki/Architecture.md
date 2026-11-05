# Architecture

```
jdbc_ingestion_config (Delta)          jdbc_ingestion_watermark (Delta)
        │                                          │
        ▼                                          │
IngestionConfigPlanner                             │
  └─ one for-each input per enabled table          │
        │                                          │
        ▼                                          │
JdbcIngestionOrchestrator                          │
  └─ one worker per config row                     │
        │                                          │
        ├─ TimestampWatermarkIngestionWorker ───────┤ read/write watermark state
        └─ EpicCsaIngestionWorker ─────────────────┘
                │
                ▼
        Bronze: staging Delta tables  (staging_table_fqn, append-only changes)
                │
                ▼
        CDC SCD2 pipeline  (cdc_pipeline.py / pipeline_bootstrap.py)
                │
                ▼
        Silver: SCD Type 2 history tables  (target_table_fqn)
```

Staging tables are the Bronze layer: raw changed rows exactly as read from the source. The `AUTO CDC` targets are the Silver layer: deduplicated SCD Type 2 history per business key. Gold models are built downstream. Full loads without `auto_cdc_from_snapshot` skip staging and the pipeline: they overwrite `target_table_fqn` directly.

A Lakeflow Job runs one ingestion group:

1. **`ingestion_configs`** — `notebooks/Task - Plan Configs` publishes the `table_configs`, `full_refresh_targets` and `full_refresh_count` task values
2. **`ingestion_worker`** — for-each task running `notebooks/Task - Run Ingestion` once per table (concurrency set by `foreach_concurrency`)
3. **`has_full_refresh_targets`** — condition task that lets step 4 run only when the planner flagged at least one table
4. **`selective_pipeline_refresh`** — `src/watersync/utils/selective_pipeline_refresh` fully refreshes only the pipeline tables flagged `FULL_REFRESH`
5. **`cdc_scd2_pipeline`** — pipeline task that applies Bronze staged changes to the Silver SCD Type 2 targets; it runs whether or not step 4 was skipped

Steps 3 to 5 are added only when the group has incremental or snapshot-CDC sources. Jobs created by the Python `IngestionJobProvisioner` do not include steps 3 and 4.
