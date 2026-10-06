from __future__ import annotations

import itertools
import random
from datetime import datetime, timedelta

try:
    import psycopg2
except (ImportError, OSError):
    psycopg2 = None  # Not available in pipeline serverless context

try:
    from databricks.sdk import WorkspaceClient
except (ImportError, OSError):
    WorkspaceClient = None

try:
    from databricks.sdk.service.postgres import Project, ProjectSpec
except (ImportError, ModuleNotFoundError):
    Project = None
    ProjectSpec = None


def _require_psycopg2():
    if psycopg2 is None:
        raise ImportError(
            "psycopg2 is required for LakebaseTestDatabaseSetup but is not installed. "
            "Install it with: pip install psycopg2-binary"
        )


class LakebaseTestDatabaseSetup:
    # Source tables that receive an EPIC-style CSA shadow table in epic_util.
    EPIC_CSA_KEY_COLUMNS: dict[str, dict[str, str]] = {
        "customers": {"customer_id": "INTEGER"},
        "products": {"product_id": "INTEGER"},
        "orders": {"order_id": "INTEGER", "line_id": "INTEGER"},
    }
    EPIC_CSA_MODIFIED_COLUMNS: dict[str, str] = {
        "customers": "updated_at",
        "products": "last_changed",
        "orders": "modified_at",
    }

    def __init__(
        self,
        project_id: str = "watersync-jdbc-test",
        project_display_name: str = "WaterSync JDBC Test DB",
        database_name: str = "databricks_postgres",
        csa_schema: str = "epic_util",
        workspace_client: "WorkspaceClient | None" = None,
    ):
        if WorkspaceClient is None:
            raise ImportError("databricks-sdk is required for LakebaseTestDatabaseSetup")
        self.w = workspace_client or WorkspaceClient()
        self.project_id = project_id
        self.project_display_name = project_display_name
        self.database_name = database_name
        self.csa_schema = csa_schema
        self.username = self.w.current_user.me().user_name

    def ensure_project(self):
        for project in itertools.islice(self.w.postgres.list_projects(page_size=50), 100):
            if project.name == f"projects/{self.project_id}":
                return project
        operation = self.w.postgres.create_project(
            project=Project(spec=ProjectSpec(display_name=self.project_display_name, pg_version=17)),
            project_id=self.project_id,
        )
        return operation.wait()

    def resolve_endpoint(self):
        branch = next(iter(self.w.postgres.list_branches(parent=f"projects/{self.project_id}")))
        endpoint = next(iter(self.w.postgres.list_endpoints(parent=branch.name)))
        return branch, endpoint

    def generate_token(self, endpoint_name: str) -> str:
        return self.w.postgres.generate_database_credential(endpoint=endpoint_name).token

    def connection(self):
        _require_psycopg2()
        _, endpoint = self.resolve_endpoint()
        token = self.generate_token(endpoint.name)
        return psycopg2.connect(
            host=endpoint.status.hosts.host,
            port=5432,
            dbname=self.database_name,
            user=self.username,
            password=token,
            sslmode="require",
        )

    def create_standard_tables(self, include_epic_csa: bool = True) -> None:
        with self.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS dbo")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS dbo.customers (
                        customer_id SERIAL PRIMARY KEY,
                        first_name VARCHAR(100),
                        last_name VARCHAR(100),
                        email VARCHAR(255),
                        city VARCHAR(100),
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS dbo.products (
                        product_id SERIAL PRIMARY KEY,
                        sku VARCHAR(50),
                        product_name VARCHAR(255),
                        category VARCHAR(100),
                        price NUMERIC(12, 2),
                        last_changed TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS dbo.orders (
                        order_id INTEGER NOT NULL,
                        line_id INTEGER NOT NULL,
                        customer_id INTEGER,
                        product_id INTEGER,
                        quantity INTEGER,
                        order_status VARCHAR(50),
                        modified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (order_id, line_id)
                    )
                """)

        if include_epic_csa:
            self.create_epic_csa_tables()

    def seed_standard_data(self, customer_count: int = 200, product_count: int = 100, order_count: int = 500, include_epic_csa: bool = True) -> None:
        first_names = ["Alice", "Bob", "Carol", "David", "Eva", "Frank", "Grace", "Henry"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Davis", "Miller"]
        cities = ["New York", "London", "Berlin", "Paris", "Milan", "Toronto", "Sydney"]
        categories = ["Hardware", "Software", "Services", "Support"]
        statuses = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED"]

        with self.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM dbo.customers")
                if cur.fetchone()[0] == 0:
                    for _ in range(customer_count):
                        first_name = random.choice(first_names)
                        last_name = random.choice(last_names)
                        cur.execute(
                            "INSERT INTO dbo.customers(first_name, last_name, email, city, updated_at) VALUES (%s, %s, %s, %s, %s)",
                            (first_name, last_name, f"{first_name.lower()}.{last_name.lower()}@example.com", random.choice(cities), datetime.now() - timedelta(days=random.randint(1, 60))),
                        )
                cur.execute("SELECT COUNT(*) FROM dbo.products")
                if cur.fetchone()[0] == 0:
                    for idx in range(product_count):
                        cur.execute(
                            "INSERT INTO dbo.products(sku, product_name, category, price, last_changed) VALUES (%s, %s, %s, %s, %s)",
                            (f"SKU-{idx:04d}", f"Product {idx:04d}", random.choice(categories), round(random.uniform(10, 500), 2), datetime.now() - timedelta(days=random.randint(1, 60))),
                        )
                cur.execute("SELECT COUNT(*) FROM dbo.orders")
                if cur.fetchone()[0] == 0:
                    for order_id in range(1, order_count + 1):
                        line_total = random.randint(1, 4)
                        for line_id in range(1, line_total + 1):
                            cur.execute(
                                "INSERT INTO dbo.orders(order_id, line_id, customer_id, product_id, quantity, order_status, modified_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                (order_id, line_id, random.randint(1, customer_count), random.randint(1, product_count), random.randint(1, 5), random.choice(statuses), datetime.now() - timedelta(days=random.randint(1, 60))),
                            )

        if include_epic_csa:
            self.seed_epic_csa_data()

    def simulate_updates(self, customer_rows: int = 3, product_rows: int = 2, order_rows: int = 5, delete_rows: int = 0) -> None:
        statuses = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "RETURNED"]
        cities = ["New York", "London", "Berlin", "Paris", "Milan", "Toronto", "Sydney"]
        with self.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                for _ in range(customer_rows):
                    cur.execute(
                        "UPDATE dbo.customers SET city = %s, updated_at = %s WHERE customer_id = (SELECT customer_id FROM dbo.customers ORDER BY random() LIMIT 1)",
                        (random.choice(cities), datetime.now() - timedelta(seconds=random.randint(1, 30))),
                    )
                for _ in range(product_rows):
                    cur.execute(
                        "UPDATE dbo.products SET price = price + 1, last_changed = %s WHERE product_id = (SELECT product_id FROM dbo.products ORDER BY random() LIMIT 1)",
                        (datetime.now() - timedelta(seconds=random.randint(1, 30)),),
                    )
                for _ in range(order_rows):
                    cur.execute(
                        "UPDATE dbo.orders SET order_status = %s, modified_at = %s WHERE (order_id, line_id) = (SELECT order_id, line_id FROM dbo.orders ORDER BY random() LIMIT 1)",
                        (random.choice(statuses), datetime.now() - timedelta(seconds=random.randint(1, 30))),
                    )
                for _ in range(delete_rows):
                    cur.execute(
                        "DELETE FROM dbo.orders WHERE (order_id, line_id) = (SELECT order_id, line_id FROM dbo.orders ORDER BY random() LIMIT 1)"
                    )

    # ------------------------------------------------------------------
    # EPIC CSA change-tracking tables (epic_util.csa_<table>)
    # ------------------------------------------------------------------
    def csa_table_name(self, base_table: str) -> str:
        """Mirror EpicCsaIngestionWorker.derive_csa_table_name()."""
        return f"{self.csa_schema}.csa_{base_table.split('.')[-1].lower()}"

    def _csa_trigger_function_sql(self) -> str:
        """PL/pgSQL trigger that writes one CSA event row per DML statement."""
        return f"""
            CREATE OR REPLACE FUNCTION {self.csa_schema}.record_csa_change() RETURNS trigger
            LANGUAGE plpgsql AS $csa$
            DECLARE
                csa_table   text := TG_ARGV[0];
                key_columns text[] := string_to_array(TG_ARGV[1], ',');
                payload     jsonb;
                key_column  text;
                value_list  text := '';
                change_ts   timestamp := clock_timestamp();
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    payload := to_jsonb(OLD);
                ELSE
                    payload := to_jsonb(NEW);
                END IF;

                FOREACH key_column IN ARRAY key_columns LOOP
                    IF value_list <> '' THEN
                        value_list := value_list || ', ';
                    END IF;
                    value_list := value_list || quote_nullable(payload ->> key_column);
                END LOOP;

                EXECUTE format(
                    'INSERT INTO %s (%s, _is_deleted, _update_dt, _timestamp_extract_key) VALUES (%s, %L, %L, %s)',
                    csa_table,
                    array_to_string(key_columns, ', '),
                    value_list,
                    (TG_OP = 'DELETE'),
                    change_ts,
                    (EXTRACT(EPOCH FROM change_ts) * 1000000)::bigint
                );

                IF TG_OP = 'DELETE' THEN
                    RETURN OLD;
                END IF;
                RETURN NEW;
            END;
            $csa$
        """

    def create_epic_csa_tables(self) -> None:
        """Create epic_util.csa_<table> shadow tables plus the triggers that fill them.

        Metadata columns are created unquoted so Postgres folds them to lower case
        (_is_deleted, _update_dt, _timestamp_extract_key); that is what the unquoted
        csa._IS_DELETED / csa._UPDATE_DT / csa._TIMESTAMP_EXTRACT_KEY references in
        EpicCsaIngestionWorker resolve to when pushed down over JDBC.
        """
        with self.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.csa_schema}")
                cur.execute(self._csa_trigger_function_sql())
                for base_table, key_columns in self.EPIC_CSA_KEY_COLUMNS.items():
                    csa_table = self.csa_table_name(base_table)
                    key_ddl = ", ".join(f"{name} {dtype} NOT NULL" for name, dtype in key_columns.items())
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {csa_table} (
                            csa_event_id BIGSERIAL PRIMARY KEY,
                            {key_ddl},
                            _is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
                            _update_dt TIMESTAMP NOT NULL DEFAULT clock_timestamp(),
                            _timestamp_extract_key BIGINT NOT NULL
                        )
                    """)
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS csa_{base_table}_extract_key_idx "
                        f"ON {csa_table} (_timestamp_extract_key)"
                    )
                    trigger_name = f"csa_{base_table}_trg"
                    cur.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON dbo.{base_table}")
                    cur.execute(f"""
                        CREATE TRIGGER {trigger_name}
                        AFTER INSERT OR UPDATE OR DELETE ON dbo.{base_table}
                        FOR EACH ROW EXECUTE FUNCTION {self.csa_schema}.record_csa_change(
                            '{csa_table}', '{",".join(key_columns)}'
                        )
                    """)

    def seed_epic_csa_data(self) -> None:
        """Backfill one CSA event per existing source row (skipped when CSA already has rows)."""
        with self.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                for base_table, key_columns in self.EPIC_CSA_KEY_COLUMNS.items():
                    csa_table = self.csa_table_name(base_table)
                    modified_column = self.EPIC_CSA_MODIFIED_COLUMNS[base_table]
                    cur.execute(f"SELECT COUNT(*) FROM {csa_table}")
                    if cur.fetchone()[0] > 0:
                        continue
                    key_list = ", ".join(key_columns)
                    cur.execute(f"""
                        INSERT INTO {csa_table} ({key_list}, _is_deleted, _update_dt, _timestamp_extract_key)
                        SELECT {key_list}, FALSE, {modified_column},
                               (EXTRACT(EPOCH FROM {modified_column}) * 1000000)::BIGINT
                        FROM dbo.{base_table}
                    """)

    def simulate_epic_csa_changes(self, customer_rows: int = 3, product_rows: int = 2, order_rows: int = 5, delete_rows: int = 2) -> None:
        """Generate updates and deletes on the source tables; triggers emit the CSA events."""
        self.simulate_updates(
            customer_rows=customer_rows,
            product_rows=product_rows,
            order_rows=order_rows,
            delete_rows=delete_rows,
        )

    def epic_csa_status(self) -> list[dict[str, str]]:
        """Per-CSA-table event count and current _timestamp_extract_key watermark."""
        status: list[dict[str, str]] = []
        with self.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                for base_table in self.EPIC_CSA_KEY_COLUMNS:
                    csa_table = self.csa_table_name(base_table)
                    cur.execute(
                        f"SELECT COUNT(*), COALESCE(MAX(_timestamp_extract_key), -1), "
                        f"COUNT(*) FILTER (WHERE _is_deleted) FROM {csa_table}"
                    )
                    event_count, max_key, delete_count = cur.fetchone()
                    status.append(
                        {
                            "csa_table": csa_table,
                            "event_count": str(event_count),
                            "delete_event_count": str(delete_count),
                            "max_timestamp_extract_key": str(max_key),
                        }
                    )
        return status

    def epic_csa_config_rows(self) -> list[dict[str, str]]:
        """Ingestion-config rows for CSA-driven incremental ingestion of the test tables."""
        settings = self.jdbc_settings()
        return [
            {
                "source_table_name": f"dbo.{base_table}",
                "csa_table_name": self.csa_table_name(base_table),
                "ingestion_type": "incremental",
                "epic_csa_enabled": "true",
                "key_columns": ",".join(key_columns),
                "watermark_column": "",
                "partition_column": next(iter(key_columns)),
                "jdbc_url": settings["jdbc_url"],
                "jdbc_user": settings["jdbc_user"],
            }
            for base_table, key_columns in self.EPIC_CSA_KEY_COLUMNS.items()
        ]

    def jdbc_settings(self) -> dict[str, str]:
        _, endpoint = self.resolve_endpoint()
        host = endpoint.status.hosts.host
        return {
            "jdbc_url": f"jdbc:postgresql://{host}:5432/{self.database_name}?sslmode=require",
            "jdbc_user": self.username,
            "jdbc_password_hint": f"w.postgres.generate_database_credential(endpoint=\'{endpoint.name}\').token",
            "jdbc_driver": "org.postgresql.Driver",
            "fetch_size": "10000",
            "num_partitions": "8",
        }
