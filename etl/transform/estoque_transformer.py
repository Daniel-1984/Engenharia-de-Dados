"""
ATLAS ERP Pipeline — Inventory Movements Transformer

Business rules validated:
  - tipo_mov     in VALID_TIPO_MOV
  - cod_produto  not null
  - data_mov     parseable
  - valor_unitario > 0
  - SAIDA/TRANSFERENCIA: quantity treated as absolute (stored negative for math)
  - AJUSTE: quantity can be negative (write-down)
  - num_documento required for ENTRADA/SAIDA; optional for AJUSTE
"""
from __future__ import annotations

import pandas as pd

from config.logging_config import get_logger
from config.settings import VALID_TIPO_MOV
from etl.transform.base_transformer import BaseTransformer

logger = get_logger("transform.estoque")


class EstoqueTransformer(BaseTransformer):

    def __init__(self):
        super().__init__(
            entity_name="movimentacoes_estoque",
            date_columns=["data_mov"],
            numeric_columns=["quantidade", "valor_unitario"],
            dedup_keys=["num_mov"],
        )

    def _apply_business_rules(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:

        violations: list[pd.Series] = []

        # Rule 1 — valid movement type
        mask_tipo = ~df["tipo_mov"].isin(VALID_TIPO_MOV)
        violations.append(mask_tipo)
        if mask_tipo.any():
            logger.warning(
                f"[estoque] Unknown tipo_mov: {df.loc[mask_tipo, 'tipo_mov'].unique()}"
            )

        # Rule 2 — product required
        mask_prod = df["cod_produto"].isna()
        violations.append(mask_prod)

        # Rule 3 — value must be positive
        mask_val = df["valor_unitario"].isna() | (df["valor_unitario"] <= 0)
        violations.append(mask_val)

        # Rule 4 — ENTRADA/SAIDA require a document reference
        mask_doc = (
            df["tipo_mov"].isin({"ENTRADA", "SAIDA"})
            & df["num_documento"].isna()
        )
        violations.append(mask_doc)
        if mask_doc.any():
            logger.warning(
                f"[estoque] {mask_doc.sum()} ENTRADA/SAIDA rows without num_documento"
            )

        # Rule 5 — date must be parseable
        mask_date = df["data_mov"].isna()
        violations.append(mask_date)

        invalid_mask = violations[0]
        for m in violations[1:]:
            invalid_mask = invalid_mask | m

        invalid_df = df[invalid_mask].copy()
        valid_df   = df[~invalid_mask].copy()

        # Derived columns
        # qty_sinal: positive for ENTRADA/AJUSTE(+), negative for SAIDA/TRANSFERENCIA
        valid_df["qty_sinal"] = valid_df.apply(
            lambda r: r["quantidade"]
            if r["tipo_mov"] in {"ENTRADA", "AJUSTE"}
            else -abs(r["quantidade"]),
            axis=1,
        )
        valid_df["valor_total_mov"] = (
            abs(valid_df["quantidade"]) * valid_df["valor_unitario"]
        ).round(2)

        return valid_df, invalid_df
