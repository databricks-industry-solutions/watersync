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

`create_standard_tables()` also creates a mock of Epic's stored procedure in `epic_util`, to test the [stored-procedure approach](Running-Ingestion#epicstoredprocedureingestionworker). Use `setup.epic_csa_config_rows(epic_change_method="stored_procedure")` for config rows that use it.

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
