"""
ATLAS ERP Pipeline — Analytics Runner

Executes SQL queries against the warehouse and prints formatted results.
Can be run standalone after the pipeline completes.

Usage:
    python -m reports.analytics_runner
    python -m reports.analytics_runner --query vendas_por_periodo
"""
from __future__ import annotations

import argparse
import sqlite3
import textwrap
from pathlib import Path

import pandas as pd

from config.logging_config import get_logger, setup_logging
from config.settings import DATABASE_PATH, BASE_DIR

setup_logging("atlas")
logger = get_logger("reports.analytics")

QUERIES_DIR = BASE_DIR / "queries"

REPORT_CATALOG: dict[str, tuple[str, str]] = {
    # key: (sql_file_path, display_title)
    "estoque_atual": (
        "operacional/estoque_atual.sql",
        "Posicao de Estoque Atual",
    ),
    "pedidos_em_aberto": (
        "operacional/pedidos_em_aberto.sql",
        "Pedidos de Venda em Aberto",
    ),
    "produtos_abaixo_minimo": (
        "operacional/produtos_abaixo_minimo.sql",
        "Produtos Abaixo do Estoque Minimo — Alerta de Reposicao",
    ),
    "vendas_por_periodo": (
        "gerencial/vendas_por_periodo.sql",
        "Receita de Vendas por Periodo",
    ),
    "top_clientes": (
        "gerencial/top_clientes.sql",
        "Top Clientes por Receita (Classificacao ABC)",
    ),
    "giro_estoque": (
        "gerencial/giro_estoque.sql",
        "Giro de Estoque por Produto",
    ),
    "margem_por_produto": (
        "gerencial/margem_por_produto.sql",
        "Margem Bruta por Produto",
    ),
}


class AnalyticsRunner:

    def __init__(self, db_path: Path = DATABASE_PATH):
        self.db_path = db_path

    def run_all(self) -> None:
        for key in REPORT_CATALOG:
            self.run_report(key)

    def run_report(self, key: str) -> pd.DataFrame | None:
        if key not in REPORT_CATALOG:
            logger.error(f"Unknown report key: {key}")
            return None

        sql_file, title = REPORT_CATALOG[key]
        sql_path = QUERIES_DIR / sql_file

        if not sql_path.exists():
            logger.error(f"SQL file not found: {sql_path}")
            return None

        sql = sql_path.read_text(encoding="utf-8")

        try:
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query(sql, conn)
        except Exception as exc:
            logger.error(f"Error running {key}: {exc}")
            return None

        self._print_report(title, df)
        return df

    @staticmethod
    def _print_report(title: str, df: pd.DataFrame) -> None:
        sep = "=" * 80
        print(f"\n{sep}")
        print(f"  {title.upper()}")
        print(f"  Rows: {len(df):,}")
        print(sep)

        if df.empty:
            print("  (no data)")
        else:
            # Format floats
            float_cols = df.select_dtypes(include="float").columns
            for col in float_cols:
                df[col] = df[col].map(
                    lambda x: f"{x:,.2f}" if pd.notna(x) else ""
                )
            print(df.to_string(index=False, max_rows=50))
        print(sep + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="ATLAS Analytics Runner")
    parser.add_argument(
        "--query",
        choices=list(REPORT_CATALOG.keys()) + ["all"],
        default="all",
        help="Which report to run (default: all)",
    )
    args = parser.parse_args()

    if not DATABASE_PATH.exists():
        print(f"[ERROR] Database not found: {DATABASE_PATH}")
        print("  Run the pipeline first:  python main.py")
        return

    runner = AnalyticsRunner()
    if args.query == "all":
        runner.run_all()
    else:
        runner.run_report(args.query)


if __name__ == "__main__":
    main()
