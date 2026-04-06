"""
ATLAS ERP Pipeline — Data Quality Validator Unit Tests
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pytest

from quality.validators import DataQualityValidator


class TestDataQualityValidator:

    def _sample_df(self):
        return pd.DataFrame({
            "cod": ["A001", "A002", "A003"],
            "nome": ["Alpha", "Beta", "Gamma"],
            "valor": [100.0, 200.0, 300.0],
            "_source_file": ["f.csv"] * 3,
        })

    def test_clean_dataframe_all_pass(self):
        df = self._sample_df()
        result = DataQualityValidator("test").run(df, required_columns=["cod", "nome"])
        assert result["overall"] == "PASS"
        assert result["failed"] == 0

    def test_missing_required_column_fails(self):
        df = self._sample_df().drop(columns=["nome"])
        result = DataQualityValidator("test").run(df, required_columns=["cod", "nome"])
        fail_checks = [c for c in result["checks"] if c["status"] == "FAIL"]
        assert any("required_columns" in c["check_name"] for c in fail_checks)

    def test_nulls_above_threshold_warns(self):
        df = self._sample_df()
        df.loc[0, "valor"] = None
        df.loc[1, "valor"] = None  # 2/3 = 67% null → FAIL
        result = DataQualityValidator("test").run(df)
        warn_or_fail = [
            c for c in result["checks"]
            if c["check_name"] == "null_check:valor"
            and c["status"] in ("WARN", "FAIL")
        ]
        assert len(warn_or_fail) == 1

    def test_duplicates_detected(self):
        df = self._sample_df()
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)  # add duplicate row
        result = DataQualityValidator("test").run(df)
        dup_check = next(c for c in result["checks"] if c["check_name"] == "duplicate_rows")
        assert dup_check["failed_rows"] == 1
        assert dup_check["status"] in ("WARN", "FAIL")

    def test_no_nulls_pass(self):
        df = self._sample_df()
        result = DataQualityValidator("test").run(df)
        null_checks = [c for c in result["checks"] if "null_check" in c["check_name"]]
        for c in null_checks:
            # metadata columns are skipped; data columns should have 0 nulls
            if not c["check_name"].startswith("null_check:_"):
                assert c["status"] == "PASS"

    def test_check_numeric_range(self):
        s = pd.Series([1, 5, 10, -1, 200])
        out_of_range = DataQualityValidator.check_numeric_range(s, min_val=0, max_val=100)
        assert out_of_range.sum() == 2  # -1 and 200

    def test_check_domain(self):
        s = pd.Series(["UN", "CX", "INVALID", "UN"])
        invalid = DataQualityValidator.check_domain(s, {"UN", "CX", "RS"})
        assert invalid.sum() == 1
