"""
ATLAS API — Endpoints de Estoque
"""
from typing import Optional
import sqlite3

from fastapi import APIRouter, Depends, Query

from api.database import get_db

router = APIRouter(prefix="/estoque", tags=["Estoque"])


@router.get("/", summary="Posicao atual do estoque")
def get_estoque(
    status: Optional[str] = Query(None, description="Filtrar por CRITICO | ALERTA | OK"),
    categoria: Optional[str] = Query(None, description="Filtrar por categoria"),
    conn: sqlite3.Connection = Depends(get_db),
):
    """
    Retorna a posicao atual do estoque de todos os produtos.

    - **status**: CRITICO (abaixo do minimo), ALERTA (menos de 20% acima do minimo), OK
    - **categoria**: nome da categoria para filtrar
    """
    sql = "SELECT * FROM vw_estoque_atual WHERE 1=1"
    params: list = []

    if status:
        sql += " AND status_estoque = ?"
        params.append(status.upper())
    if categoria:
        sql += " AND categoria = ?"
        params.append(categoria)

    sql += " ORDER BY status_estoque, saldo_atual"

    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


@router.get("/criticos", summary="Produtos em estado critico de estoque")
def get_criticos(conn: sqlite3.Connection = Depends(get_db)):
    """Atalho: retorna apenas produtos CRITICOS (saldo <= estoque_minimo)."""
    rows = conn.execute(
        "SELECT * FROM vw_estoque_atual WHERE status_estoque = 'CRITICO' ORDER BY saldo_atual"
    ).fetchall()
    return [dict(r) for r in rows]


@router.get("/resumo", summary="Resumo por categoria")
def get_resumo(conn: sqlite3.Connection = Depends(get_db)):
    """Saldo total, valor em estoque e contagem de SKUs por categoria."""
    rows = conn.execute("""
        SELECT
            categoria,
            COUNT(*)                          AS total_skus,
            SUM(saldo_atual)                  AS saldo_total,
            ROUND(SUM(valor_estoque), 2)      AS valor_total,
            SUM(CASE WHEN status_estoque = 'CRITICO' THEN 1 ELSE 0 END) AS criticos,
            SUM(CASE WHEN status_estoque = 'ALERTA'  THEN 1 ELSE 0 END) AS alertas
        FROM vw_estoque_atual
        GROUP BY categoria
        ORDER BY valor_total DESC
    """).fetchall()
    return [dict(r) for r in rows]


@router.get("/{cod_produto}", summary="Estoque de um produto especifico")
def get_produto(cod_produto: str, conn: sqlite3.Connection = Depends(get_db)):
    """Retorna o saldo atual e historico de movimentacoes de um produto."""
    estoque = conn.execute(
        "SELECT * FROM vw_estoque_atual WHERE cod_produto = ?", (cod_produto.upper(),)
    ).fetchone()

    if not estoque:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Produto {cod_produto} nao encontrado")

    movimentacoes = conn.execute("""
        SELECT m.num_documento, m.tipo_mov, m.quantidade, m.data_movimentacao, m.observacao
        FROM fct_movimentacoes_estoque m
        WHERE m.cod_produto = ?
        ORDER BY m.data_movimentacao DESC
        LIMIT 30
    """, (cod_produto.upper(),)).fetchall()

    return {
        "estoque": dict(estoque),
        "ultimas_movimentacoes": [dict(r) for r in movimentacoes],
    }
