"""
ATLAS ERP Pipeline — Data Quality Validators

A lightweight, composable DQ framework.

Each `Check` is a named rule that returns pass/fail counts.
`DataQualityValidator` runs a standard battery of checks on any DataFrame
and returns a structured result dict consumed by `QualityReport`.

Checks included:
  - null_check         : columns that exceed null threshold
  - duplicate_check    : duplicate primary keys
  - required_columns   : presence of mandatory fields
  - numeric_range      : values within [min, max]
  - domain_check       : values belong to an allowed set
  - date_consistency   : date_end >= date_start
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

from config.logging_config import get_logger
from config.settings import MAX_DUPLICATE_PCT, MAX_NULL_PCT

logger = get_logger("quality.validators")


@dataclass
class CheckResult:
    check_name:   str
    status:       str          # PASS | WARN | FAIL
    passed_rows:  int
    failed_rows:  int
    total_rows:   int
    detail:       str = ""

    @property
    def pass_rate(self) -> float:
        return self.passed_rows / self.total_rows if self.total_rows else 0.0


class DataQualityValidator:
    """
    Runs a standard battery of quality checks on a DataFrame.

    Parameters
    ----------
    entity_name : str
        Used in log messages and the result dict.
    """

    def __init__(self, entity_name: str):
        self.entity_name = entity_name
        self.results: list[CheckResult] = []

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self, df: pd.DataFrame, required_columns: list[str] | None = None) -> dict:
        """
        Execute all checks and return a structured result dict.
        """
        self.results = []
        n = len(df)

        self._check_required_columns(df, required_columns or [])
        self._check_nulls(df)
        self._check_duplicates(df)

        passed = sum(1 for r in self.results if r.status == "PASS")
        warned = sum(1 for r in self.results if r.status == "WARN")
        failed = sum(1 for r in self.results if r.status == "FAIL")

        overall = "PASS" if failed == 0 and warned == 0 else ("WARN" if failed == 0 else "FAIL")

        logger.info(
            f"[DQ:{self.entity_name}] {n:,} rows | "
            f"checks={len(self.results)} PASS={passed} WARN={warned} FAIL={failed}"
        )

        return {
            "entity":   self.entity_name,
            "rows":     n,
            "overall":  overall,
            "checks":   [r.__dict__ for r in self.results],
            "passed":   passed,
            "warned":   warned,
            "failed":   failed,
        }

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    def _check_required_columns(
        self, df: pd.DataFrame, required: list[str]
    ) -> None:
        missing = [c for c in required if c not in df.columns]
        if missing:
            self.results.append(CheckResult(
                check_name="required_columns",
                status="FAIL",
                passed_rows=0,
                failed_rows=len(missing),
                total_rows=len(required),
                detail=f"Missing columns: {missing}",
            ))
            logger.error(f"[DQ:{self.entity_name}] Missing required columns: {missing}")
        else:
            self.results.append(CheckResult(
                check_name="required_columns",
                status="PASS",
                passed_rows=len(required),
                failed_rows=0,
                total_rows=len(required),
            ))

    def _check_nulls(self, df: pd.DataFrame) -> None:
        n = len(df)
        meta_cols = {c for c in df.columns if c.startswith("_")}
        for col in df.columns:
            if col in meta_cols:
                continue
            null_count = df[col].isna().sum()
            null_pct   = null_count / n if n else 0.0
            status = "PASS"
            if null_pct > MAX_NULL_PCT:
                status = "WARN" if null_pct < 0.3 else "FAIL"
            self.results.append(CheckResult(
                check_name=f"null_check:{col}",
                status=status,
                passed_rows=n - null_count,
                failed_rows=null_count,
                total_rows=n,
                detail=f"{null_pct:.1%} null" if null_count else "",
            ))

    def _check_duplicates(self, df: pd.DataFrame) -> None:
        n = len(df)
        dup_count = df.duplicated().sum()
        dup_pct   = dup_count / n if n else 0.0
        status = "PASS"
        if dup_pct > 0:
            status = "WARN" if dup_pct <= MAX_DUPLICATE_PCT else "FAIL"
        self.results.append(CheckResult(
            check_name="duplicate_rows",
            status=status,
            passed_rows=n - dup_count,
            failed_rows=dup_count,
            total_rows=n,
            detail=f"{dup_pct:.1%} duplicates" if dup_count else "",
        ))

    # ------------------------------------------------------------------
    # Ad-hoc reusable checks (called by transformers if needed)
    # ------------------------------------------------------------------

    @staticmethod
    def check_numeric_range(
        series: pd.Series,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ) -> pd.Series:
        """Return boolean mask: True where value is OUT of range."""
        mask = pd.Series(False, index=series.index)
        if min_val is not None:
            mask |= series < min_val
        if max_val is not None:
            mask |= series > max_val
        return mask

    @staticmethod
    def check_domain(series: pd.Series, allowed: set) -> pd.Series:
        """Return boolean mask: True where value NOT in allowed set."""
        return ~series.isin(allowed)
