# Lakebase Test Setup

Provision a Lakebase Postgres project seeded with `customers`, `products`, and `orders` tables, plus matching `epic_util.csa_*` tables kept up to date by triggers, to test both workers without a real source system:

```python
from watersync.utils import LakebaseTestDatabaseSetup

setup = LakebaseTestDatabaseSetup(
    project_id="watersync-jdbc-test",
    project_display_name="WaterSync JDBC Test DB",
)
setup.ensure_project()
setup.create_standard_tables()
setup.seed_standard_data(customer_count=200, product_count=100, order_count=500)

jdbc_settings = setup.jdbc_settings()      # JDBC URL and credentials for config rows
csa_rows = setup.epic_csa_config_rows()    # suggested EPIC CSA config rows
```

`create_standard_tables()` also creates `epic_util.esp_cr_altered_rows`, a Postgres mock of Epic's changed-rows stored procedure for testing the [stored-procedure worker](Running-Ingestion#epicstoredprocedureingestionworker). It takes the same parameters as Epic's `ESP_CR_ALTERED_ROWS`, reads `epic_util.csa_<table>`, and fills a session temp table with the key columns and `cr_upd_date` for rows changed in `[i_startdate, i_enddate)`. `i_noduplicates = 1` keeps only the latest change per key and `i_skipupdatedate = 1` leaves out `cr_upd_date`. The other parameters are accepted but ignored.

```sql
CALL epic_util.esp_cr_altered_rows('customers', now()::timestamp - interval '1 day', now()::timestamp, 'changed', 1);
SELECT * FROM changed;
```

It also creates `epic_util.esp_cr_altered_rows_select(i_tablename, i_startdate, i_enddate, i_noduplicates)`, which calls the procedure and returns each temp-table row as `jsonb`. The worker reads Postgres sources through it, because the Postgres JDBC driver can't return a `SELECT` that follows a `CALL` in the same statement.

Use `setup.epic_csa_config_rows(epic_change_method="stored_procedure")` for config rows that read through the mock.

Simulate ongoing changes:

```python
setup.simulate_updates()
setup.simulate_epic_csa_changes(delete_rows=2)
```

Or via the CLI:

```bash
watersync-setup-lakebase \
  --project-id watersync-jdbc-test \
  --customer-count 200 \
  --order-count 500 \
  --simulate-updates
```
