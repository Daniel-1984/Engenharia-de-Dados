"""
ATLAS ERP Pipeline — CSV Reader (Extract layer)

Responsible for:
  - Reading CSV files from the raw data directory
  - Detecting encoding and schema
  - Adding pipeline metadata columns
  - Logging ingestion metrics
"""
from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from config.logging_config import get_logger
from config.settings import CSV_SEPARATOR, ENCODING

logger = get_logger("extract.csv_reader")


class CSVReadError(Exception):
    """Raised when a source file cannot be read or has an invalid schema."""


class CSVReader:
    """
    Reads a single CSV file and returns an enriched DataFrame.

    Parameters
    ----------
    file_path : Path
        Absolute path to the source CSV.
    required_columns : list[str] | None
        Column names that MUST be present; raises CSVReadError if any is missing.
    """

    METADATA_COLUMNS = ["_source_file", "_ingestion_ts", "_row_hash", "_pipeline_run_id"]

    def __init__(
        self,
        file_path: Path,
        required_columns: Optional[list[str]] = None,
        pipeline_run_id: Optional[str] = None,
    ):
        self.file_path      = Path(file_path)
        self.required_columns = required_columns or []
        self.pipeline_run_id  = pipeline_run_id or datetime.now().strftime("%Y%m%d_%H%M%S")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read(self) -> pd.DataFrame:
        """
        Read the file and return a DataFrame with metadata columns appended.

        Returns
        -------
        pd.DataFrame
            Raw data + 4 metadata columns (_source_file, _ingestion_ts, …)

        Raises
        ------
        CSVReadError
            If the file is missing, empty, or lacks required columns.
        """
        self._assert_file_exists()

        logger.info(f"Reading file: {self.file_path.name}")
        try:
            df = pd.read_csv(
                self.file_path,
                sep=CSV_SEPARATOR,
                encoding=ENCODING,
                dtype=str,          # read everything as str; transformers handle casting
                keep_default_na=False,
                na_values=["", "NULL", "null", "N/A", "NA", "NaN"],
            )
        except Exception as exc:
            raise CSVReadError(f"Failed to read {self.file_path}: {exc}") from exc

        if df.empty:
            raise CSVReadError(f"File is empty: {self.file_path.name}")

        self._validate_schema(df)
        df = self._add_metadata(df)

        logger.info(
            f"Extracted {len(df):,} rows | {len(df.columns)} columns | "
            f"file={self.file_path.name}"
        )
        return df

    def preview(self, n: int = 5) -> pd.DataFrame:
        """Quick peek at the file — reads only the first n rows."""
        return pd.read_csv(
            self.file_path,
            sep=CSV_SEPARATOR,
            encoding=ENCODING,
            nrows=n,
            dtype=str,
            keep_default_na=False,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _assert_file_exists(self) -> None:
        if not self.file_path.exists():
            raise CSVReadError(f"File not found: {self.file_path}")

    def _validate_schema(self, df: pd.DataFrame) -> None:
        missing = [c for c in self.required_columns if c not in df.columns]
        if missing:
            raise CSVReadError(
                f"Required columns missing in {self.file_path.name}: {missing}"
            )
        logger.debug(f"Schema OK — columns: {list(df.columns)}")

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        now = datetime.now().isoformat(timespec="seconds")
        df["_source_file"]    = self.file_path.name
        df["_ingestion_ts"]   = now
        df["_pipeline_run_id"] = self.pipeline_run_id
        # stable hash per row (useful for deduplication across runs)
        df["_row_hash"] = df.apply(
            lambda row: hashlib.md5(
                "|".join(str(v) for v in row.values).encode()
            ).hexdigest(),
            axis=1,
        )
        return df
