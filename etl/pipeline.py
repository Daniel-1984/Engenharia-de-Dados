"""
ATLAS ERP Pipeline — Orchestrator

Drives the full ETL cycle:
  Extract → Transform → Validate → Load (Staging) → Load (Operational)

Each step is timed and logged. A single pipeline_run_id ties all records
together in the control tables for end-to-end lineage.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

from config.logging_config import get_logger, setup_logging
from config.settings import SOURCE_FILES
from etl.extract.csv_reader import CSVReader, CSVReadError
from etl.load.sqlite_loader import SQLiteLoader
from etl.transform.clientes_transformer import ClientesTransformer
from etl.transform.estoque_transformer import EstoqueTransformer
from etl.transform.pedidos_transformer import (
    ItensPedidoCompraTransformer,
    ItensPedidoVendaTransformer,
    PedidosCompraTransformer,
    PedidosVendaTransformer,
)
from etl.transform.produtos_transformer import ProdutosTransformer
from quality.validators import DataQualityValidator
from quality.quality_report import QualityReport

setup_logging("atlas")
logger = get_logger("pipeline")


@dataclass
class PipelineMetrics:
    pipeline_run_id: str
    started_at: str
    finished_at: str = ""
    status: str = "RUNNING"
    entities: list[dict] = field(default_factory=list)
    total_input_rows: int = 0
    total_valid_rows: int = 0
    total_rejected_rows: int = 0
    elapsed_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)


class ERPPipeline:
    """
    Top-level pipeline class.

    >>> pipeline = ERPPipeline()
    >>> metrics  = pipeline.run()
    """

    # Maps each source to (transformer_class, staging_table, operational_table, required_cols)
    PIPELINE_STEPS: list[tuple[str, Any, str, str, list[str]]] = [
        (
            "produtos",
            ProdutosTransformer,
            "stg_produtos",
            "dim_produtos",
            ["cod_produto", "descricao", "unidade", "custo_unitario", "preco_venda"],
        ),
        (
            "clientes",
            ClientesTransformer,
            "stg_clientes",
            "dim_clientes",
            ["cod_cliente", "razao_social"],
        ),
        (
            "fornecedores",
            None,           # no domain-specific transformer — pass-through
            "stg_fornecedores",
            "dim_fornecedores",
            ["cod_fornecedor", "razao_social"],
        ),
        (
            "pedidos_venda",
            PedidosVendaTransformer,
            "stg_pedidos_venda",
            "fct_pedidos_venda",
            ["num_pedido", "cod_cliente", "data_pedido", "status"],
        ),
        (
            "itens_pedido_venda",
            ItensPedidoVendaTransformer,
            "stg_itens_pedido_venda",
            "fct_itens_pedido_venda",
            ["num_pedido", "seq_item", "cod_produto", "quantidade", "preco_unitario"],
        ),
        (
            "pedidos_compra",
            PedidosCompraTransformer,
            "stg_pedidos_compra",
            "fct_pedidos_compra",
            ["num_oc", "cod_fornecedor", "data_oc", "status"],
        ),
        (
            "itens_pedido_compra",
            ItensPedidoCompraTransformer,
            "stg_itens_pedido_compra",
            "fct_itens_pedido_compra",
            ["num_oc", "seq_item", "cod_produto", "quantidade", "custo_unitario"],
        ),
        (
            "movimentacoes_estoque",
            EstoqueTransformer,
            "stg_movimentacoes_estoque",
            "fct_movimentacoes_estoque",
            ["num_mov", "data_mov", "tipo_mov", "cod_produto", "quantidade"],
        ),
    ]

    def __init__(self):
        self.run_id  = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
        self.metrics = PipelineMetrics(
            pipeline_run_id=self.run_id,
            started_at=datetime.now().isoformat(),
        )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> PipelineMetrics:
        t_start = time.perf_counter()
        logger.info("=" * 70)
        logger.info(f"ATLAS Pipeline started | run_id={self.run_id}")
        logger.info("=" * 70)

        dq_reports: list[dict] = []

        with SQLiteLoader() as loader:
            loader.initialize_schema()

            for (entity, transformer_cls, stg_table, ops_table, required_cols) in self.PIPELINE_STEPS:
                self._process_entity(
                    entity=entity,
                    transformer_cls=transformer_cls,
                    stg_table=stg_table,
                    ops_table=ops_table,
                    required_cols=required_cols,
                    loader=loader,
                    dq_reports=dq_reports,
                )

            self._build_dim_tempo(loader)

        # Print quality summary
        report = QualityReport(dq_reports)
        report.print_summary()

        elapsed = time.perf_counter() - t_start
        self.metrics.finished_at     = datetime.now().isoformat()
        self.metrics.status          = "SUCCESS" if not self.metrics.errors else "PARTIAL"
        self.metrics.elapsed_seconds = round(elapsed, 2)

        logger.info("=" * 70)
        logger.info(
            f"Pipeline {self.metrics.status} | "
            f"run_id={self.run_id} | "
            f"elapsed={elapsed:.2f}s | "
            f"total_valid={self.metrics.total_valid_rows:,} | "
            f"total_rejected={self.metrics.total_rejected_rows:,}"
        )
        logger.info("=" * 70)
        return self.metrics

    # ------------------------------------------------------------------
    # Per-entity processing
    # ------------------------------------------------------------------

    def _process_entity(
        self,
        entity: str,
        transformer_cls,
        stg_table: str,
        ops_table: str,
        required_cols: list[str],
        loader: SQLiteLoader,
        dq_reports: list[dict],
    ) -> None:
        t0 = time.perf_counter()
        logger.info(f"--- Processing: {entity} ---")

        # 1. Extract
        try:
            reader = CSVReader(
                SOURCE_FILES[entity],
                required_columns=required_cols,
                pipeline_run_id=self.run_id,
            )
            raw_df = reader.read()
        except CSVReadError as exc:
            logger.error(f"[{entity}] Extract failed: {exc}")
            self.metrics.errors.append(str(exc))
            loader.log_execution(self.run_id, entity, "EXTRACT_ERROR", 0, 0, 0, 0, str(exc))
            return

        # 2. Data quality check (pre-transform)
        dq = DataQualityValidator(entity)
        dq_result = dq.run(raw_df, required_cols)
        dq_reports.append(dq_result)

        # 3. Transform
        if transformer_cls is not None:
            transformer  = transformer_cls()
            result       = transformer.transform(raw_df)
            valid_df     = result.valid
            rejected_df  = result.rejected
            input_rows   = result.metrics["input_rows"]
            valid_rows   = result.metrics["valid_rows"]
            rejected_rows = result.metrics["rejected_rows"]
        else:
            # pass-through (no transformer defined)
            from etl.transform.base_transformer import BaseTransformer
            valid_df      = raw_df.copy()
            rejected_df   = pd.DataFrame()
            input_rows    = len(raw_df)
            valid_rows    = len(raw_df)
            rejected_rows = 0

        # 4. Load staging (full refresh)
        loader.load(raw_df, stg_table, mode="replace")

        # 5. Load operational (upsert)
        loader.load(valid_df, ops_table, mode="upsert")

        # 6. Persist rejected rows
        if not rejected_df.empty:
            loader.load_rejected(rejected_df, entity)

        # 7. Log execution
        elapsed = time.perf_counter() - t0
        loader.log_execution(
            pipeline_run_id=self.run_id,
            entity=entity,
            status="SUCCESS",
            input_rows=input_rows,
            valid_rows=valid_rows,
            rejected_rows=rejected_rows,
            elapsed_seconds=elapsed,
        )

        self.metrics.entities.append({
            "entity": entity, "input": input_rows,
            "valid": valid_rows, "rejected": rejected_rows,
        })
        self.metrics.total_input_rows    += input_rows
        self.metrics.total_valid_rows    += valid_rows
        self.metrics.total_rejected_rows += rejected_rows

    # ------------------------------------------------------------------
    # Dimension: time
    # ------------------------------------------------------------------

    def _build_dim_tempo(self, loader: SQLiteLoader) -> None:
        """Generate a date dimension for 2024-01-01 → 2025-12-31."""
        logger.info("Building dim_tempo")
        dates = pd.date_range("2024-01-01", "2025-12-31", freq="D")
        dim = pd.DataFrame({
            "data_key":       dates.strftime("%Y%m%d").astype(int),
            "data":           dates.strftime("%Y-%m-%d"),
            "ano":            dates.year,
            "semestre":       ((dates.month - 1) // 6 + 1),
            "trimestre":      dates.quarter,
            "mes":            dates.month,
            "nome_mes":       dates.strftime("%B"),
            "semana_ano":     dates.isocalendar().week.astype(int),
            "dia":            dates.day,
            "dia_semana":     dates.dayofweek + 1,
            "nome_dia":       dates.strftime("%A"),
            "is_fim_semana":  (dates.dayofweek >= 5).astype(int),
            "_dw_loaded_at":  datetime.now().isoformat(),
            "_is_active":     1,
        })
        loader.load(dim, "dim_tempo", mode="replace")
        logger.info(f"dim_tempo loaded with {len(dim):,} rows")
