"""
ATLAS ERP Pipeline — SQLite Loader (Load layer)

Responsibilities:
  - Create/connect to the SQLite warehouse
  - Execute schema DDL (idempotent — uses CREATE TABLE IF NOT EXISTS)
  - Load staging tables (full truncate-and-reload per run)
  - Load operational/dimensional tables (upsert via INSERT OR REPLACE)
  - Log every batch load to log_etl_execucoes
  - Persist rejected rows to log_registros_rejeitados
"""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from config.logging_config import get_logger
from config.settings import DATABASE_PATH, DATABASE_DIR

logger = get_logger("load.sqlite")


class SQLiteLoader:
    """
    Manages all write operations to the SQLite warehouse.

    Usage
    -----
    >>> with SQLiteLoader() as loader:
    ...     loader.initialize_schema()
    ...     loader.load(df, table="stg_produtos", mode="replace")
    """

    def __init__(self, db_path: Path = DATABASE_PATH):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "SQLiteLoader":
        DATABASE_DIR.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        # FK enforcement is disabled during ETL load; referential integrity
        # is validated at the DQ layer before loading.
        self._conn.execute("PRAGMA foreign_keys=OFF")
        logger.debug(f"SQLite connected: {self.db_path}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._conn:
            if exc_type is None:
                self._conn.commit()
            else:
                self._conn.rollback()
                logger.error(f"Transaction rolled back due to: {exc_val}")
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def initialize_schema(self) -> None:
        """Execute schema.sql to create all tables (idempotent)."""
        schema_file = DATABASE_DIR / "schema.sql"
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file}")

        with open(schema_file, encoding="utf-8") as f:
            sql = f.read()

        self._conn.executescript(sql)
        self._conn.commit()
        logger.info("Schema initialized")

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def load(
        self,
        df: pd.DataFrame,
        table: str,
        mode: str = "append",
        chunk_size: int = 1_000,
    ) -> int:
        """
        Load a DataFrame into `table`.

        Parameters
        ----------
        df : pd.DataFrame
        table : str
            Target table name.
        mode : str
            'replace' → DROP + recreate  (used for staging)
            'append'  → INSERT (keeping existing rows)
            'upsert'  → INSERT OR REPLACE (requires PK on table)
        chunk_size : int
            Rows per SQLite transaction batch.

        Returns
        -------
        int
            Number of rows loaded.
        """
        if df.empty:
            logger.warning(f"Empty DataFrame — nothing loaded to {table}")
            return 0

        start = time.perf_counter()
        df_clean = self._prepare_df(df)

        if mode == "replace":
            self._conn.execute(f"DELETE FROM {table}")
            self._conn.commit()

        total = 0
        for i in range(0, len(df_clean), chunk_size):
            chunk = df_clean.iloc[i : i + chunk_size]
            if_exists = "append"
            if mode == "upsert":
                self._upsert_chunk(chunk, table)
            else:
                chunk.to_sql(table, self._conn, if_exists=if_exists, index=False)
            total += len(chunk)

        elapsed = time.perf_counter() - start
        logger.info(
            f"Loaded {total:,} rows → {table} | mode={mode} | {elapsed:.2f}s"
        )
        return total

    def load_rejected(self, df: pd.DataFrame, entity: str) -> None:
        """Persist rejected rows to the audit table."""
        if df.empty:
            return
        records = [
            {
                "entity":        entity,
                "reject_reason": row.get("_reject_reason", "UNKNOWN"),
                "row_data":      str(row.to_dict()),
                "pipeline_run":  row.get("_pipeline_run_id", ""),
                "logged_at":     datetime.now().isoformat(),
            }
            for _, row in df.iterrows()
        ]
        reject_df = pd.DataFrame(records)
        reject_df.to_sql(
            "log_registros_rejeitados", self._conn, if_exists="append", index=False
        )
        logger.debug(f"Persisted {len(reject_df)} rejected rows for {entity}")

    def log_execution(
        self,
        pipeline_run_id: str,
        entity: str,
        status: str,
        input_rows: int,
        valid_rows: int,
        rejected_rows: int,
        elapsed_seconds: float,
        error_message: str = "",
    ) -> None:
        """Write an ETL run record to the control table."""
        self._conn.execute(
            """
            INSERT INTO log_etl_execucoes
              (pipeline_run_id, entity, status, input_rows, valid_rows,
               rejected_rows, elapsed_seconds, error_message, executed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pipeline_run_id, entity, status, input_rows, valid_rows,
                rejected_rows, round(elapsed_seconds, 3), error_message,
                datetime.now().isoformat(),
            ),
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def query(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        """Execute a SELECT and return results as DataFrame."""
        return pd.read_sql_query(sql, self._conn, params=params)

    def execute_script(self, sql: str) -> None:
        self._conn.executescript(sql)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    @staticmethod
    def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
        """Drop internal metadata columns that don't belong in the DB,
        and convert Timestamp columns to ISO strings for SQLite compatibility."""
        meta = {"_source_file", "_ingestion_ts", "_row_hash",
                "_pipeline_run_id", "_reject_reason"}
        keep = [c for c in df.columns if c not in meta]
        out = df[keep].copy()
        # SQLite doesn't understand pandas Timestamp — convert to str
        for col in out.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]", "datetimetz"]).columns:
            out[col] = out[col].dt.strftime("%Y-%m-%d").where(out[col].notna(), other=None)
        # Also handle object columns that may contain Timestamp objects
        for col in out.select_dtypes(include="object").columns:
            if out[col].apply(lambda x: hasattr(x, 'strftime')).any():
                out[col] = out[col].apply(
                    lambda x: x.strftime("%Y-%m-%d") if hasattr(x, 'strftime') else x
                )
        return out

    def _upsert_chunk(self, chunk: pd.DataFrame, table: str) -> None:
        cols   = list(chunk.columns)
        ph     = ", ".join(["?"] * len(cols))
        col_str = ", ".join(cols)
        sql    = f"INSERT OR REPLACE INTO {table} ({col_str}) VALUES ({ph})"
        self._conn.executemany(sql, chunk.itertuples(index=False, name=None))
        self._conn.commit()
