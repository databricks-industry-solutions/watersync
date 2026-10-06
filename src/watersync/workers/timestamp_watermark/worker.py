from __future__ import annotations

import logging
from datetime import datetime, timedelta

from watersync.models import ReadResult
from watersync.sql_dialect import sql_exists_subquery, watermark_window_predicate
from watersync.workers.base import JdbcIngestionWorker

logger = logging.getLogger(__name__)


class TimestampWatermarkIngestionWorker(JdbcIngestionWorker):
    def read_source(self) -> ReadResult:
        _ctx = {
            "ingestion_group": self.config.ingestion_group,
            "source_table": self.config.source_table_name,
        }
        # Full-refresh override: read entire source, bypassing watermark.
        # Triggered by the global runtime flag OR a per-table FULL_REFRESH
        # status in the watermark table (set from the control-plane UI).
        if self.effective_full_refresh:
            logger.info(
                "[READ]   %s — full_refresh requested, performing full source read",
                self.config.source_table_name,
                extra=_ctx,
            )
            return ReadResult(df=self.read_source_jdbc_standard(None))

        if self.config.ingestion_type != "incremental":
            logger.info(
                "[READ]   %s — full load (no watermark)",
                self.config.source_table_name,
                extra=_ctx,
            )
            return ReadResult(df=self.read_source_jdbc_standard(None))

        last_watermark = self.get_last_watermark()
        cutoff = (
            datetime.now() - timedelta(minutes=self.config.watermark_threshold_minutes)
        ).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(
            "[READ]   %s — incremental window  last_wm=%s  cutoff=%s",
            self.config.source_table_name,
            last_watermark,
            cutoff,
            extra=_ctx,
        )
        exists_query = sql_exists_subquery(
            self.config.source_table_name,
            watermark_window_predicate(
                self.config.watermark_column,
                last_watermark,
                cutoff,
                self.sql_dialect,
            ),
            self.sql_dialect,
        )
        has_rows = self.build_jdbc_reader(exists_query).load().first() is not None
        if not has_rows:
            logger.info(
                "[READ]   %s — existence check: no rows in window, will skip",
                self.config.source_table_name,
                extra=_ctx,
            )
            return ReadResult(df=None, skip=True)
        logger.info(
            "[READ]   %s — new rows found, proceeding with JDBC read",
            self.config.source_table_name,
            extra=_ctx,
        )
        return ReadResult(df=self.read_source_jdbc_standard(last_watermark))
