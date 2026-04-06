"""
ATLAS ERP Pipeline — Base Transformer (Template Method pattern)

All domain transformers extend this class and implement `_apply_business_rules`.
The `transform()` method drives the standard pipeline:
  1. Copy input
  2. Standardize column names
  3. Parse dates
  4. Cast numerics
  5. Trim / upper strings
  6. Handle nulls
  7. Deduplicate
  8. Apply domain-specific business rules  ← overridden by subclass
  9. Collect & return rejected rows
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd

from config.logging_config import get_logger
from config.settings import DATE_FORMAT, DATE_FORMATS_ALT, REJECTED_DATA_DIR

logger = get_logger("transform.base")


class TransformResult:
    """Container returned by every transformer."""

    def __init__(self, valid: pd.DataFrame, rejected: pd.DataFrame, metrics: dict):
        self.valid    = valid
        self.rejected = rejected
        self.metrics  = metrics

    def __repr__(self) -> str:
        return (
            f"<TransformResult valid={len(self.valid):,} "
            f"rejected={len(self.rejected):,} "
            f"metrics={self.metrics}>"
        )


class BaseTransformer(ABC):
    """
    Abstract base transformer. Subclasses must implement `_apply_business_rules`.

    Parameters
    ----------
    entity_name : str
        Identifier used in logs and reject-file names.
    date_columns : list[str]
        Columns to be parsed as dates.
    numeric_columns : list[str]
        Columns to be cast to float64.
    integer_columns : list[str]
        Columns to be cast to Int64 (nullable integer).
    dedup_keys : list[str]
        Columns that identify a unique row; duplicates are rejected.
    """

    def __init__(
        self,
        entity_name: str,
        date_columns: Optional[list[str]] = None,
        numeric_columns: Optional[list[str]] = None,
        integer_columns: Optional[list[str]] = None,
        dedup_keys: Optional[list[str]] = None,
    ):
        self.entity_name     = entity_name
        self.date_columns    = date_columns or []
        self.numeric_columns = numeric_columns or []
        self.integer_columns = integer_columns or []
        self.dedup_keys      = dedup_keys or []
        self._rejected_rows: list[pd.DataFrame] = []

    # ------------------------------------------------------------------
    # Public entry point (Template Method)
    # ------------------------------------------------------------------

    def transform(self, df: pd.DataFrame) -> TransformResult:
        logger.info(f"[{self.entity_name}] Starting transformation — {len(df):,} rows")
        df = df.copy()

        df = self._standardize_column_names(df)
        df = self._parse_dates(df)
        df = self._cast_numerics(df)
        df = self._trim_strings(df)
        df, dupes = self._deduplicate(df)

        if not dupes.empty:
            self._add_rejected(dupes, reason="DUPLICATE_KEY")

        df, invalid = self._apply_business_rules(df)

        if not invalid.empty:
            self._add_rejected(invalid, reason="BUSINESS_RULE_VIOLATION")

        df = self._add_dw_metadata(df)

        rejected_df = (
            pd.concat(self._rejected_rows, ignore_index=True)
            if self._rejected_rows
            else pd.DataFrame()
        )

        self._save_rejected(rejected_df)

        metrics = {
            "entity":      self.entity_name,
            "input_rows":  len(df) + len(rejected_df),
            "valid_rows":  len(df),
            "rejected_rows": len(rejected_df),
            "reject_rate": round(len(rejected_df) / max(len(df) + len(rejected_df), 1), 4),
        }

        logger.info(
            f"[{self.entity_name}] Done — valid={metrics['valid_rows']:,} "
            f"rejected={metrics['rejected_rows']:,} "
            f"rate={metrics['reject_rate']:.1%}"
        )
        return TransformResult(valid=df, rejected=rejected_df, metrics=metrics)

    # ------------------------------------------------------------------
    # Abstract hook — subclasses implement domain logic here
    # ------------------------------------------------------------------

    @abstractmethod
    def _apply_business_rules(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validate domain-specific rules.

        Returns
        -------
        (valid_df, invalid_df)
        """

    # ------------------------------------------------------------------
    # Standard transformation steps
    # ------------------------------------------------------------------

    @staticmethod
    def _standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r"\s+", "_", regex=True)
            .str.replace(r"[^\w]", "", regex=True)
        )
        return df

    def _parse_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        all_formats = [DATE_FORMAT] + DATE_FORMATS_ALT
        for col in self.date_columns:
            if col not in df.columns:
                continue
            parsed = None
            for fmt in all_formats:
                try:
                    parsed = pd.to_datetime(df[col], format=fmt, errors="coerce")
                    if parsed.notna().sum() > parsed.isna().sum():
                        break
                except Exception:
                    continue
            if parsed is not None:
                # Count failures only on rows that had a non-null original value
                original_notnull = df[col].notna()
                df[col] = parsed
                failed = (original_notnull & df[col].isna()).sum()
                if failed:
                    logger.warning(
                        f"[{self.entity_name}] {failed} unparseable dates in '{col}'"
                    )
        return df

    def _cast_numerics(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", "."), errors="coerce"
                )
        for col in self.integer_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        return df

    @staticmethod
    def _trim_strings(df: pd.DataFrame) -> pd.DataFrame:
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())
        return df

    def _deduplicate(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        if not self.dedup_keys:
            return df, pd.DataFrame()
        valid_keys = [k for k in self.dedup_keys if k in df.columns]
        if not valid_keys:
            return df, pd.DataFrame()
        dupes_mask = df.duplicated(subset=valid_keys, keep="first")
        dupes = df[dupes_mask].copy()
        if not dupes.empty:
            logger.warning(
                f"[{self.entity_name}] {len(dupes)} duplicate rows removed "
                f"on keys {valid_keys}"
            )
        return df[~dupes_mask], dupes

    @staticmethod
    def _add_dw_metadata(df: pd.DataFrame) -> pd.DataFrame:
        from datetime import datetime
        df["_dw_loaded_at"] = datetime.now().isoformat(timespec="seconds")
        df["_is_active"]    = 1
        return df

    # ------------------------------------------------------------------
    # Reject helpers
    # ------------------------------------------------------------------

    def _add_rejected(self, df: pd.DataFrame, reason: str) -> None:
        tmp = df.copy()
        tmp["_reject_reason"] = reason
        self._rejected_rows.append(tmp)

    def _save_rejected(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        REJECTED_DATA_DIR.mkdir(parents=True, exist_ok=True)
        from datetime import datetime
        ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = REJECTED_DATA_DIR / f"{self.entity_name}_rejected_{ts}.csv"
        df.to_csv(out, index=False, encoding="utf-8")
        logger.warning(
            f"[{self.entity_name}] {len(df)} rejected rows saved → {out.name}"
        )
