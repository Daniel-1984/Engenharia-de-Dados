"""
ATLAS ERP Pipeline — Quality Report

Aggregates DQ results from all entities and prints a formatted summary table.
"""
from __future__ import annotations

from config.logging_config import get_logger

logger = get_logger("quality.report")

_STATUS_ICON = {"PASS": "OK", "WARN": "!!", "FAIL": "XX"}
_STATUS_LABEL = {"PASS": "PASS", "WARN": "WARN", "FAIL": "FAIL"}


class QualityReport:
    """
    Parameters
    ----------
    reports : list[dict]
        List of result dicts returned by DataQualityValidator.run()
    """

    def __init__(self, reports: list[dict]):
        self.reports = reports

    def print_summary(self) -> None:
        print("\n" + "=" * 72)
        print("  ATLAS ERP PIPELINE — DATA QUALITY REPORT")
        print("=" * 72)
        print(f"  {'Entity':<28} {'Rows':>8}  {'Checks':>7}  {'PASS':>6}  {'WARN':>6}  {'FAIL':>6}  Status")
        print("-" * 72)

        total_pass = total_warn = total_fail = 0
        for r in self.reports:
            icon = _STATUS_ICON.get(r["overall"], "?")
            print(
                f"  {r['entity']:<28} {r['rows']:>8,}  "
                f"{len(r['checks']):>7}  "
                f"{r['passed']:>6}  "
                f"{r['warned']:>6}  "
                f"{r['failed']:>6}  "
                f"[{icon}] {r['overall']}"
            )
            total_pass += r["passed"]
            total_warn += r["warned"]
            total_fail += r["failed"]

        print("-" * 72)
        total_checks = total_pass + total_warn + total_fail
        print(
            f"  {'TOTAL':<28} {'':>8}  {total_checks:>7}  "
            f"{total_pass:>6}  {total_warn:>6}  {total_fail:>6}"
        )
        print("=" * 72)

        if total_fail > 0:
            print(f"\n  [!] {total_fail} FAILED check(s) — review rejected/ directory")
        elif total_warn > 0:
            print(f"\n  [!] {total_warn} WARNING(s) — review logs for details")
        else:
            print("\n  All quality checks passed.")

        print("=" * 72 + "\n")

        # Detailed failures
        for r in self.reports:
            fails = [c for c in r["checks"] if c["status"] in ("FAIL", "WARN")]
            for f in fails:
                logger.warning(
                    f"[DQ] {r['entity']}.{f['check_name']} "
                    f"status={f['status']} "
                    f"failed={f['failed_rows']} "
                    f"detail={f['detail']}"
                )
