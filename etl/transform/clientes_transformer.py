"""
ATLAS ERP Pipeline — Clients Transformer

Business rules validated:
  - razao_social    not null
  - cnpj            not null; cleaned to digits only; 14-digit length
  - limite_credito  >= 0  (null → 0.0)
  - ativo           in {0, 1}
  - data_cadastro   parseable date
"""
from __future__ import annotations

import re

import pandas as pd

from config.logging_config import get_logger
from etl.transform.base_transformer import BaseTransformer

logger = get_logger("transform.clientes")

_CNPJ_DIGITS = re.compile(r"\D")


def _clean_cnpj(value: str) -> str:
    """Strip non-digit characters from CNPJ."""
    if pd.isna(value):
        return ""
    return _CNPJ_DIGITS.sub("", str(value))


class ClientesTransformer(BaseTransformer):

    def __init__(self):
        super().__init__(
            entity_name="clientes",
            date_columns=["data_cadastro"],
            numeric_columns=["limite_credito"],
            integer_columns=["ativo"],
            dedup_keys=["cod_cliente"],
        )

    def _apply_business_rules(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:

        # ---- Clean CNPJ first (formatting artefacts) ----
        df["cnpj_digits"] = df["cnpj"].apply(_clean_cnpj)

        violations: list[pd.Series] = []

        # Rule 1 — razao_social required
        mask_name = df["razao_social"].isna() | (df["razao_social"].str.strip() == "")
        violations.append(mask_name)

        # Rule 2 — CNPJ required and must have exactly 14 digits
        mask_cnpj = df["cnpj"].isna() | (df["cnpj_digits"].str.len() != 14)
        violations.append(mask_cnpj)
        if mask_cnpj.any():
            logger.warning(
                f"[clientes] {mask_cnpj.sum()} rows with missing/invalid CNPJ: "
                f"{df.loc[mask_cnpj, 'cod_cliente'].tolist()}"
            )

        # Rule 3 — limite_credito must be >= 0 (fill nulls with 0)
        df["limite_credito"] = df["limite_credito"].fillna(0.0)
        mask_credit = df["limite_credito"] < 0
        violations.append(mask_credit)

        invalid_mask = violations[0]
        for m in violations[1:]:
            invalid_mask = invalid_mask | m

        invalid_df = df[invalid_mask].copy()
        valid_df   = df[~invalid_mask].copy()

        # Normalise: formatted CNPJ  XX.XXX.XXX/XXXX-XX
        valid_df["cnpj"] = valid_df["cnpj_digits"].apply(
            lambda d: f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"
            if len(d) == 14 else d
        )
        valid_df = valid_df.drop(columns=["cnpj_digits"])
        invalid_df = invalid_df.drop(columns=["cnpj_digits"], errors="ignore")

        valid_df["razao_social"] = valid_df["razao_social"].str.upper()
        valid_df["segmento"]     = valid_df["segmento"].str.upper()
        valid_df["estado"]       = valid_df["estado"].str.upper()

        return valid_df, invalid_df
