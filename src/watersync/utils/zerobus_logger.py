from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any


class ZerobusLogHandler(logging.Handler):
    """A logging.Handler that pushes records to a Delta table via ZeroBus Ingest SDK.

    Records are sent fire-and-forget so logging never blocks the main ingestion flow.
    Call .close() (or use as a context manager) to flush and close the gRPC stream.

    The target Delta table uses two fixed primary-key columns and a single VARIANT
    payload column, so new log fields can be added at any time without a schema
    migration.  Expected schema (created by UnityCatalogSetup.create_log_table)::

        run_id        STRING  -- primary filter key; UUID shared across one run
        log_timestamp STRING  -- UTC ISO-8601 timestamp of the log event
        payload       VARIANT -- full log record as nested JSON

    The payload object structure::

        {
            "level":        "INFO",
            "logger_name":  "watersync.workers.base",
            "message": {
                "text":   "...",       -- formatted log message string
                "module": "base",      -- Python module name
                "func":   "process",   -- function name
                "lineno": 42,          -- source line number
                "exc":    null         -- exception text, or null
            },
            "ingestion_group": "epic",          -- present when context is available
            "source_table":    "epic.patients"  -- present when context is available
        }

    Query examples (Databricks SQL colon syntax)::

        -- All errors for a run
        SELECT log_timestamp,
               payload:logger_name::STRING       AS logger,
               payload:message:text::STRING      AS message,
               payload:message:lineno::INT        AS lineno,
               payload:source_table::STRING      AS source_table
        FROM   watersync_logs
        WHERE  run_id = '<uuid>'
          AND  payload:level::STRING = 'ERROR'
        ORDER BY log_timestamp;

        -- Ingestion timeline for one table
        SELECT log_timestamp, payload:message:text::STRING AS message
        FROM   watersync_logs
        WHERE  payload:source_table::STRING = 'epic.patients'
        ORDER BY log_timestamp;
    """

    def __init__(self, stream: Any, run_id: str | None = None) -> None:
        super().__init__()
        self._stream = stream
        self._run_id = run_id or str(uuid.uuid4())

    @property
    def run_id(self) -> str:
        return self._run_id

    def emit(self, record: logging.LogRecord) -> None:  # noqa: D102
        try:
            # message is itself a structured object — new sub-fields can be added
            # here at any time; the VARIANT column stores them automatically.
            payload: dict[str, Any] = {
                "level": record.levelname,
                "logger_name": record.name,
                "message": {
                    "text": record.getMessage(),
                    "module": record.module,
                    "func": record.funcName,
                    "lineno": record.lineno,
                    "exc": record.exc_text,  # None when no exception
                },
            }
            ingestion_group = getattr(record, "ingestion_group", None)
            source_table = getattr(record, "source_table", None)
            if ingestion_group is not None:
                payload["ingestion_group"] = ingestion_group
            if source_table is not None:
                payload["source_table"] = source_table

            row: dict[str, Any] = {
                "run_id": self._run_id,
                "log_timestamp": datetime.fromtimestamp(
                    record.created, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S.%f"),
                "payload": payload,
            }
            self._stream.ingest_record_fire_forget(row)
        except Exception:  # noqa: BLE001
            self.handleError(record)

    def close(self) -> None:  # noqa: D102
        try:
            self._stream.close()
        except Exception:  # noqa: BLE001
            pass
        finally:
            super().close()

    # ------------------------------------------------------------------
    # Context manager support: `with setup_watersync_logging(...) as h:`
    # ------------------------------------------------------------------
    def __enter__(self) -> "ZerobusLogHandler":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def setup_watersync_logging(
    server_endpoint: str,
    workspace_url: str,
    table_name: str,
    run_id: str | None = None,
    level: int = logging.INFO,
    console: bool = True,
    token_provider: Callable[[], str] | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
) -> ZerobusLogHandler:
    """Configure the watersync root logger to emit records via ZeroBus Ingest.

    All loggers under the ``watersync.*`` hierarchy inherit this configuration.

    Authentication priority (first match wins):

    1. ``client_id`` + ``client_secret`` — M2M OAuth via ZeroBus SDK (recommended
       for production; works on serverless and classic clusters alike). Retrieve
       from ``dbutils.secrets`` to avoid hardcoding::

           client_id=dbutils.secrets.get(scope="watersync", key="zb_client_id")
           client_secret=dbutils.secrets.get(scope="watersync", key="zb_client_secret")

    2. ``token_provider`` — a callable returning a Bearer token string on each call.
       Works on **classic interactive clusters** where ``Config(auth_type="runtime-oauth")``
       issues workspace-scoped tokens.  On serverless, ``runtime-oauth`` issues
       tokens for the control-plane org, which ZeroBus rejects.

    3. No credentials supplied — falls back to ``Config(auth_type="runtime-oauth")``
       (useful for Lakeflow Jobs running as a service principal).

    Each record is written to a Delta table as::

        run_id STRING, log_timestamp STRING, payload VARIANT

    where ``payload`` holds the full log record — including a structured
    ``message`` sub-object — as nested JSON.

    Args:
        server_endpoint: ZeroBus server endpoint URL
            (e.g. ``https://<id>.zerobus.us-west-2.cloud.databricks.com``).
        workspace_url: Databricks workspace URL
            (e.g. ``https://dbc-xxx.cloud.databricks.com``).
            Obtain from ``dbutils.notebook.entry_point.getDbutils()
            .notebook().getContext().apiUrl().get()``.
        table_name: Fully-qualified Delta table name (``catalog.schema.table``).
        run_id: Optional run identifier; a UUID is generated if omitted.
        level: Python logging level (default: ``logging.INFO``).
        console: When ``True``, also attach a StreamHandler for notebook stdout.
        token_provider: Optional zero-argument callable that returns a Bearer
            token string on each call.  Works on classic interactive clusters
            where the token is a workspace-scoped JWT.  On serverless compute
            ``ctx.apiToken()`` returns an opaque PAT which ZeroBus rejects.
        client_id: OAuth M2M client ID (service principal).  When provided
            together with ``client_secret``, the SDK’s native ``create_stream``
            path is used and handles token exchange internally.
        client_secret: OAuth M2M client secret paired with ``client_id``.

    Returns:
        The :class:`ZerobusLogHandler` instance.  Call ``.close()`` (or use as a
        context manager) when the run is complete to flush and close the stream.

    Example::

        handler = setup_watersync_logging(
            server_endpoint=ZEROBUS_ENDPOINT,
            workspace_url=WORKSPACE_URL,
            table_name="main.default.watersync_logs",
        )
        try:
            JdbcIngestionOrchestrator(spark, runtime).run_selected_ingestion()
        finally:
            handler.close()
    """
    from databricks.sdk.config import Config
    from zerobus.sdk.shared import (
        RecordType,
        StreamConfigurationOptions,
        TableProperties,
    )
    from zerobus.sdk.sync import ZerobusSdk

    class _RuntimeOAuthHeadersProvider:
        """Injects credentials into every ZeroBus gRPC call.

        If a ``token_provider`` callable is supplied it is called on every
        ``get_headers()`` invocation, ensuring the stream survives token expiry.
        On serverless notebooks, pass ``lambda: ctx.apiToken().get()`` so the
        workspace-scoped token is used (the control-plane ``runtime-oauth``
        token carries a different audience and is rejected by ZeroBus).
        Without a provider, falls back to ``Config(auth_type='runtime-oauth')``.
        """

        def __init__(self, tbl: str) -> None:
            self._table_name = tbl
            self._provider = token_provider  # captured from outer scope
            if self._provider is None:
                self._cfg = Config(auth_type="runtime-oauth", scopes=["all-apis"])
            else:
                self._cfg = None

        def get_headers(self) -> list[tuple[str, str]]:
            if self._provider is not None:
                tok = self._provider()
            else:
                tok = self._cfg.oauth_token().access_token  # type: ignore[union-attr]
            return [
                ("authorization", f"Bearer {tok}"),
                ("x-databricks-zerobus-table-name", self._table_name),
            ]

    sdk = ZerobusSdk(server_endpoint, workspace_url)
    if client_id and client_secret:
        # M2M OAuth — the SDK handles token exchange and audience internally.
        # This is the only path that reliably works on serverless compute.
        stream = sdk.create_stream(
            client_id,
            client_secret,
            TableProperties(table_name),
            StreamConfigurationOptions(record_type=RecordType.JSON),
        )
    else:
        # Headers-provider path: token_provider callable or runtime-oauth fallback.
        # Works on classic interactive clusters; on serverless the runtime-oauth
        # token carries the control-plane org audience and is rejected by ZeroBus.
        stream = sdk.create_stream(
            "",  # ignored when headers_provider is supplied
            "",  # ignored when headers_provider is supplied
            TableProperties(table_name),
            StreamConfigurationOptions(record_type=RecordType.JSON),
            headers_provider=_RuntimeOAuthHeadersProvider(table_name),
        )

    zb_handler = ZerobusLogHandler(stream, run_id=run_id)
    zb_handler.setLevel(level)

    watersync_logger = logging.getLogger("watersync")
    watersync_logger.setLevel(level)
    # Remove any previously registered ZerobusLogHandler to avoid duplicates on re-runs
    watersync_logger.handlers = [
        h for h in watersync_logger.handlers if not isinstance(h, ZerobusLogHandler)
    ]
    watersync_logger.addHandler(zb_handler)

    if console:
        # Avoid duplicate console handlers on notebook re-runs
        if not any(
            isinstance(h, logging.StreamHandler) and not isinstance(h, ZerobusLogHandler)
            for h in watersync_logger.handlers
        ):
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)
            console_handler.setFormatter(
                logging.Formatter(
                    "[%(asctime)s] %(levelname)-8s %(name)s — %(message)s",
                    datefmt="%H:%M:%S",
                )
            )
            watersync_logger.addHandler(console_handler)

    watersync_logger.propagate = False
    return zb_handler
