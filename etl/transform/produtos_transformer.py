"""
ATLAS ERP Pipeline — Products Transformer

Business rules validated:
  - custo_unitario  > 0
  - preco_venda     > 0
  - preco_venda     > custo_unitario           (margin check)
  - estoque_minimo  >= 0
  - estoque_maximo  >= estoque_minimo
  - unidade         in VALID_UNIDADES
  - ativo           in {0, 1}
"""
from __future__ import annotations

import pandas as pd

from config.logging_config import get_logger
from config.settings import VALID_UNIDADES
from etl.transform.base_transformer import BaseTransformer

logger = get_logger("transform.produtos")


class ProdutosTransformer(BaseTransformer):

    def __init__(self):
        super().__init__(
            entity_name="produtos",
            numeric_columns=["custo_unitario", "preco_venda"],
            integer_columns=["estoque_minimo", "estoque_maximo", "ativo"],
            dedup_keys=["cod_produto"],
        )

    def _apply_business_rules(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:

        violations: list[pd.Series] = []

        # Rule 1 — cost must be positive
        mask_cost = df["custo_unitario"].isna() | (df["custo_unitario"] <= 0)
        violations.append(mask_cost)
        if mask_cost.any():
            logger.warning(f"[produtos] {mask_cost.sum()} rows with custo_unitario <= 0")

        # Rule 2 — sale price must be positive
        mask_price = df["preco_venda"].isna() | (df["preco_venda"] <= 0)
        violations.append(mask_price)

        # Rule 3 — margin: price must exceed cost
        mask_margin = df["preco_venda"] <= df["custo_unitario"]
        violations.append(mask_margin)
        if mask_margin.any():
            logger.warning(f"[produtos] {mask_margin.sum()} rows with negative/zero margin")

        # Rule 4 — valid unit of measure
        mask_unit = ~df["unidade"].isin(VALID_UNIDADES)
        violations.append(mask_unit)
        if mask_unit.any():
            logger.warning(
                f"[produtos] {mask_unit.sum()} rows with unknown unidade: "
                f"{df.loc[mask_unit, 'unidade'].unique()}"
            )

        # Rule 5 — estoque_maximo >= estoque_minimo
        mask_stock = df["estoque_maximo"] < df["estoque_minimo"]
        violations.append(mask_stock)

        invalid_mask = violations[0]
        for m in violations[1:]:
            invalid_mask = invalid_mask | m

        invalid_df = df[invalid_mask].copy()
        valid_df   = df[~invalid_mask].copy()

        # Normalise descricao & categoria to UPPER
        valid_df["descricao"] = valid_df["descricao"].str.upper()
        valid_df["categoria"] = valid_df["categoria"].str.upper()

        return valid_df, invalid_df
