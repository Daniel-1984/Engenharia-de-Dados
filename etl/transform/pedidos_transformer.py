"""
ATLAS ERP Pipeline — Orders Transformer (Sales & Purchase)

Business rules — Sales Orders:
  - data_entrega_prevista >= data_pedido
  - desconto_percentual   in [0, 100]
  - status                in VALID_STATUS_VENDA
  - cod_cliente           not null

Business rules — Sales Order Items:
  - quantidade     > 0
  - preco_unitario > 0
  - desconto       in [0, 100]

Business rules — Purchase Orders:
  - data_entrega_prevista >= data_oc
  - status                in VALID_STATUS_COMPRA
  - cod_fornecedor        not null

Business rules — Purchase Order Items:
  - quantidade     > 0
  - custo_unitario > 0
"""
from __future__ import annotations

import pandas as pd

from config.logging_config import get_logger
from config.settings import VALID_STATUS_COMPRA, VALID_STATUS_VENDA
from etl.transform.base_transformer import BaseTransformer

logger = get_logger("transform.pedidos")


class PedidosVendaTransformer(BaseTransformer):

    def __init__(self):
        super().__init__(
            entity_name="pedidos_venda",
            date_columns=["data_pedido", "data_entrega_prevista", "data_entrega_real"],
            numeric_columns=["desconto_percentual"],
            dedup_keys=["num_pedido"],
        )

    def _apply_business_rules(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:

        violations: list[pd.Series] = []

        mask_client = df["cod_cliente"].isna()
        violations.append(mask_client)

        mask_status = ~df["status"].isin(VALID_STATUS_VENDA)
        violations.append(mask_status)
        if mask_status.any():
            logger.warning(
                f"[pedidos_venda] Unknown status values: "
                f"{df.loc[mask_status, 'status'].unique()}"
            )

        mask_discount = (
            df["desconto_percentual"].notna()
            & ((df["desconto_percentual"] < 0) | (df["desconto_percentual"] > 100))
        )
        violations.append(mask_discount)

        # delivery cannot be before order date
        mask_dates = (
            df["data_entrega_prevista"].notna()
            & df["data_pedido"].notna()
            & (df["data_entrega_prevista"] < df["data_pedido"])
        )
        violations.append(mask_dates)

        invalid_mask = violations[0]
        for m in violations[1:]:
            invalid_mask = invalid_mask | m

        invalid_df = df[invalid_mask].copy()
        valid_df   = df[~invalid_mask].copy()
        valid_df["desconto_percentual"] = valid_df["desconto_percentual"].fillna(0.0)

        return valid_df, invalid_df


class ItensPedidoVendaTransformer(BaseTransformer):

    def __init__(self):
        super().__init__(
            entity_name="itens_pedido_venda",
            numeric_columns=["quantidade", "preco_unitario", "desconto_item_percentual"],
            dedup_keys=["num_pedido", "seq_item"],
        )

    def _apply_business_rules(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:

        violations: list[pd.Series] = []

        mask_qty   = df["quantidade"].isna() | (df["quantidade"] <= 0)
        mask_price = df["preco_unitario"].isna() | (df["preco_unitario"] <= 0)
        violations.extend([mask_qty, mask_price])

        if mask_qty.any():
            logger.warning(f"[itens_venda] {mask_qty.sum()} rows with qty <= 0")
        if mask_price.any():
            logger.warning(f"[itens_venda] {mask_price.sum()} rows with price <= 0")

        invalid_mask = violations[0]
        for m in violations[1:]:
            invalid_mask = invalid_mask | m

        invalid_df = df[invalid_mask].copy()
        valid_df   = df[~invalid_mask].copy()

        valid_df["desconto_item_percentual"] = (
            valid_df["desconto_item_percentual"].fillna(0.0)
        )
        # computed column: total bruto por item
        valid_df["valor_total_item"] = (
            valid_df["quantidade"]
            * valid_df["preco_unitario"]
            * (1 - valid_df["desconto_item_percentual"] / 100)
        ).round(2)

        return valid_df, invalid_df


class PedidosCompraTransformer(BaseTransformer):

    def __init__(self):
        super().__init__(
            entity_name="pedidos_compra",
            date_columns=["data_oc", "data_entrega_prevista", "data_recebimento"],
            dedup_keys=["num_oc"],
        )

    def _apply_business_rules(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:

        violations: list[pd.Series] = []

        mask_supplier = df["cod_fornecedor"].isna()
        violations.append(mask_supplier)

        mask_status = ~df["status"].isin(VALID_STATUS_COMPRA)
        violations.append(mask_status)

        mask_dates = (
            df["data_entrega_prevista"].notna()
            & df["data_oc"].notna()
            & (df["data_entrega_prevista"] < df["data_oc"])
        )
        violations.append(mask_dates)

        invalid_mask = violations[0]
        for m in violations[1:]:
            invalid_mask = invalid_mask | m

        return df[~invalid_mask].copy(), df[invalid_mask].copy()


class ItensPedidoCompraTransformer(BaseTransformer):

    def __init__(self):
        super().__init__(
            entity_name="itens_pedido_compra",
            numeric_columns=["quantidade", "custo_unitario"],
            dedup_keys=["num_oc", "seq_item"],
        )

    def _apply_business_rules(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:

        mask_qty  = df["quantidade"].isna() | (df["quantidade"] <= 0)
        mask_cost = df["custo_unitario"].isna() | (df["custo_unitario"] <= 0)
        invalid_mask = mask_qty | mask_cost

        valid_df = df[~invalid_mask].copy()
        valid_df["valor_total_item"] = (
            valid_df["quantidade"] * valid_df["custo_unitario"]
        ).round(2)

        return valid_df, df[invalid_mask].copy()
