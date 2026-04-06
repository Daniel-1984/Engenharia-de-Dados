"""
ATLAS ERP Pipeline — Transformer Unit Tests
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pytest

from etl.transform.produtos_transformer import ProdutosTransformer
from etl.transform.clientes_transformer import ClientesTransformer
from etl.transform.pedidos_transformer import (
    ItensPedidoVendaTransformer,
    PedidosVendaTransformer,
)
from etl.transform.estoque_transformer import EstoqueTransformer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _base_produto(**overrides) -> dict:
    base = {
        "cod_produto": "PRD001",
        "descricao": "Produto Teste",
        "categoria": "Informatica",
        "unidade": "UN",
        "custo_unitario": "100.00",
        "preco_venda": "199.90",
        "estoque_minimo": "5",
        "estoque_maximo": "50",
        "ativo": "1",
        "cod_fornecedor_principal": "FOR001",
        "_source_file": "test.csv",
        "_ingestion_ts": "2024-01-01T00:00:00",
        "_pipeline_run_id": "test_run",
        "_row_hash": "abc123",
    }
    base.update(overrides)
    return base


def _base_cliente(**overrides) -> dict:
    base = {
        "cod_cliente": "CLI001",
        "razao_social": "Empresa Teste Ltda",
        "cnpj": "12.345.678/0001-90",
        "segmento": "Tecnologia",
        "cidade": "Sao Paulo",
        "estado": "SP",
        "limite_credito": "50000.00",
        "ativo": "1",
        "data_cadastro": "2024-01-01",
        "_source_file": "test.csv",
        "_ingestion_ts": "2024-01-01T00:00:00",
        "_pipeline_run_id": "test_run",
        "_row_hash": "def456",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# ProdutosTransformer
# ---------------------------------------------------------------------------

class TestProdutosTransformer:

    def test_valid_product_passes(self):
        df = pd.DataFrame([_base_produto()])
        result = ProdutosTransformer().transform(df)
        assert len(result.valid) == 1
        assert len(result.rejected) == 0

    def test_negative_cost_rejected(self):
        df = pd.DataFrame([_base_produto(custo_unitario="-10")])
        result = ProdutosTransformer().transform(df)
        assert len(result.rejected) == 1
        assert len(result.valid) == 0

    def test_price_less_than_cost_rejected(self):
        df = pd.DataFrame([_base_produto(custo_unitario="200", preco_venda="100")])
        result = ProdutosTransformer().transform(df)
        assert len(result.rejected) == 1

    def test_invalid_unit_rejected(self):
        df = pd.DataFrame([_base_produto(unidade="INVALID_UNIT")])
        result = ProdutosTransformer().transform(df)
        assert len(result.rejected) == 1

    def test_descricao_normalized_to_upper(self):
        df = pd.DataFrame([_base_produto(descricao="notebook dell")])
        result = ProdutosTransformer().transform(df)
        assert result.valid.iloc[0]["descricao"] == "NOTEBOOK DELL"

    def test_duplicate_product_code_rejected(self):
        df = pd.DataFrame([_base_produto(), _base_produto(descricao="Duplicate")])
        result = ProdutosTransformer().transform(df)
        assert len(result.valid) == 1
        assert len(result.rejected) == 1

    def test_metrics_correct(self):
        rows = [_base_produto(), _base_produto(cod_produto="PRD002", preco_venda="0")]
        df = pd.DataFrame(rows)
        result = ProdutosTransformer().transform(df)
        assert result.metrics["input_rows"] == 2
        assert result.metrics["valid_rows"] == 1
        assert result.metrics["rejected_rows"] == 1


# ---------------------------------------------------------------------------
# ClientesTransformer
# ---------------------------------------------------------------------------

class TestClientesTransformer:

    def test_valid_client_passes(self):
        df = pd.DataFrame([_base_cliente()])
        result = ClientesTransformer().transform(df)
        assert len(result.valid) == 1
        assert len(result.rejected) == 0

    def test_cnpj_cleaned_and_formatted(self):
        df = pd.DataFrame([_base_cliente(cnpj="12345678000190")])
        result = ClientesTransformer().transform(df)
        assert result.valid.iloc[0]["cnpj"] == "12.345.678/0001-90"

    def test_missing_cnpj_rejected(self):
        df = pd.DataFrame([_base_cliente(cnpj=None)])
        result = ClientesTransformer().transform(df)
        assert len(result.rejected) == 1

    def test_null_limit_filled_with_zero(self):
        df = pd.DataFrame([_base_cliente(limite_credito=None)])
        result = ClientesTransformer().transform(df)
        # Null limit is filled with 0, not rejected
        assert len(result.valid) == 1
        assert result.valid.iloc[0]["limite_credito"] == 0.0

    def test_missing_razao_social_rejected(self):
        df = pd.DataFrame([_base_cliente(razao_social=None)])
        result = ClientesTransformer().transform(df)
        assert len(result.rejected) == 1


# ---------------------------------------------------------------------------
# ItensPedidoVendaTransformer
# ---------------------------------------------------------------------------

class TestItensPedidoVendaTransformer:

    def _base_item(self, **overrides):
        base = {
            "num_pedido": "PV-001", "seq_item": "1",
            "cod_produto": "PRD001", "quantidade": "2",
            "preco_unitario": "199.90", "desconto_item_percentual": "0",
            "_source_file": "test.csv", "_ingestion_ts": "2024-01-01T00:00:00",
            "_pipeline_run_id": "test_run", "_row_hash": "ghi789",
        }
        base.update(overrides)
        return base

    def test_valid_item_passes(self):
        df = pd.DataFrame([self._base_item()])
        result = ItensPedidoVendaTransformer().transform(df)
        assert len(result.valid) == 1

    def test_zero_quantity_rejected(self):
        df = pd.DataFrame([self._base_item(quantidade="0")])
        result = ItensPedidoVendaTransformer().transform(df)
        assert len(result.rejected) == 1

    def test_negative_price_rejected(self):
        df = pd.DataFrame([self._base_item(preco_unitario="-50")])
        result = ItensPedidoVendaTransformer().transform(df)
        assert len(result.rejected) == 1

    def test_valor_total_item_computed(self):
        df = pd.DataFrame([self._base_item(quantidade="3", preco_unitario="100", desconto_item_percentual="10")])
        result = ItensPedidoVendaTransformer().transform(df)
        # 3 * 100 * (1 - 0.10) = 270
        assert result.valid.iloc[0]["valor_total_item"] == pytest.approx(270.0)


# ---------------------------------------------------------------------------
# EstoqueTransformer
# ---------------------------------------------------------------------------

class TestEstoqueTransformer:

    def _base_mov(self, **overrides):
        base = {
            "num_mov": "MOV-001", "data_mov": "2024-01-01",
            "tipo_mov": "ENTRADA", "cod_produto": "PRD001",
            "quantidade": "10", "valor_unitario": "100.00",
            "origem": "COMPRA", "num_documento": "OC-001",
            "observacao": "",
            "_source_file": "test.csv", "_ingestion_ts": "2024-01-01T00:00:00",
            "_pipeline_run_id": "test_run", "_row_hash": "jkl012",
        }
        base.update(overrides)
        return base

    def test_valid_entrada_passes(self):
        df = pd.DataFrame([self._base_mov()])
        result = EstoqueTransformer().transform(df)
        assert len(result.valid) == 1
        assert result.valid.iloc[0]["qty_sinal"] == 10.0

    def test_saida_has_negative_qty_sinal(self):
        df = pd.DataFrame([self._base_mov(tipo_mov="SAIDA", num_documento="PV-001")])
        result = EstoqueTransformer().transform(df)
        assert result.valid.iloc[0]["qty_sinal"] == -10.0

    def test_invalid_tipo_mov_rejected(self):
        df = pd.DataFrame([self._base_mov(tipo_mov="INVALIDO")])
        result = EstoqueTransformer().transform(df)
        assert len(result.rejected) == 1

    def test_saida_without_document_rejected(self):
        df = pd.DataFrame([self._base_mov(tipo_mov="SAIDA", num_documento=None)])
        result = EstoqueTransformer().transform(df)
        assert len(result.rejected) == 1

    def test_date_with_alternate_format_parsed(self):
        df = pd.DataFrame([self._base_mov(data_mov="01/01/2024")])
        result = EstoqueTransformer().transform(df)
        assert len(result.valid) == 1
