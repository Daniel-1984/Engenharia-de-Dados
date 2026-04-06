"""
ATLAS API — Endpoints de Vendas
"""
import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends, Query

from api.database import get_db

router = APIRouter(prefix="/vendas", tags=["Vendas"])


@router.get("/", summary="Lista pedidos de venda")
def get_pedidos(
    status: Optional[str] = Query(None, description="Filtrar por status do pedido"),
    cod_cliente: Optional[str] = Query(None, description="Filtrar por codigo do cliente"),
    limit: int = Query(50, ge=1, le=500),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Retorna pedidos de venda com dados do cliente."""
    sql = """
        SELECT
            v.num_pedido, v.cod_cliente, c.nome_cliente, c.segmento,
            v.data_pedido, v.data_entrega_prevista, v.data_entrega_real,
            v.status_pedido, v.valor_total, v.desconto_pct
        FROM fct_pedidos_venda v
        LEFT JOIN dim_clientes c ON v.cod_cliente = c.cod_cliente
        WHERE 1=1
    """
    params: list = []

    if status:
        sql += " AND v.status_pedido = ?"
        params.append(status.upper())
    if cod_cliente:
        sql += " AND v.cod_cliente = ?"
        params.append(cod_cliente.upper())

    sql += " ORDER BY v.data_pedido DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


@router.get("/kpis", summary="KPIs de vendas")
def get_kpis(conn: sqlite3.Connection = Depends(get_db)):
    """
    Retorna os principais indicadores de vendas:
    - Receita total e ticket medio
    - Pedidos por status
    - Top 5 clientes por receita
    """
    receita = conn.execute("""
        SELECT
            COUNT(*)                    AS total_pedidos,
            ROUND(SUM(valor_total), 2)  AS receita_total,
            ROUND(AVG(valor_total), 2)  AS ticket_medio,
            COUNT(DISTINCT cod_cliente) AS clientes_ativos
        FROM fct_pedidos_venda
        WHERE status_pedido NOT IN ('CANCELADO', 'DEVOLVIDO')
    """).fetchone()

    por_status = conn.execute("""
        SELECT status_pedido, COUNT(*) AS total, ROUND(SUM(valor_total), 2) AS valor
        FROM fct_pedidos_venda
        GROUP BY status_pedido
        ORDER BY total DESC
    """).fetchall()

    top_clientes = conn.execute("""
        SELECT
            v.cod_cliente,
            c.nome_cliente,
            COUNT(*)                    AS pedidos,
            ROUND(SUM(v.valor_total), 2) AS receita
        FROM fct_pedidos_venda v
        LEFT JOIN dim_clientes c ON v.cod_cliente = c.cod_cliente
        WHERE v.status_pedido NOT IN ('CANCELADO', 'DEVOLVIDO')
        GROUP BY v.cod_cliente
        ORDER BY receita DESC
        LIMIT 5
    """).fetchall()

    return {
        "resumo": dict(receita),
        "por_status": [dict(r) for r in por_status],
        "top_5_clientes": [dict(r) for r in top_clientes],
    }


@router.get("/por-periodo", summary="Receita mensal com variacao MoM")
def get_por_periodo(conn: sqlite3.Connection = Depends(get_db)):
    """Receita por mes com crescimento mes a mes (MoM) usando LAG()."""
    rows = conn.execute("""
        WITH mensal AS (
            SELECT
                strftime('%Y', data_pedido)                 AS ano,
                strftime('%m', data_pedido)                 AS mes,
                ROUND(SUM(valor_total), 2)                  AS receita,
                COUNT(*)                                     AS pedidos
            FROM fct_pedidos_venda
            WHERE status_pedido NOT IN ('CANCELADO', 'DEVOLVIDO')
            GROUP BY ano, mes
        )
        SELECT
            ano, mes, receita, pedidos,
            LAG(receita) OVER (ORDER BY ano, mes)           AS receita_mes_anterior,
            ROUND(
                (receita - LAG(receita) OVER (ORDER BY ano, mes))
                / NULLIF(LAG(receita) OVER (ORDER BY ano, mes), 0) * 100,
            2)                                              AS variacao_pct
        FROM mensal
        ORDER BY ano, mes
    """).fetchall()
    return [dict(r) for r in rows]


@router.get("/margem", summary="Margem bruta por produto")
def get_margem(conn: sqlite3.Connection = Depends(get_db)):
    """Receita, CMV e margem bruta por produto, ordenado por maior receita."""
    rows = conn.execute("""
        SELECT
            p.cod_produto,
            p.descricao,
            p.categoria,
            SUM(i.quantidade)                               AS unidades_vendidas,
            ROUND(SUM(i.valor_total_item), 2)               AS receita_bruta,
            ROUND(SUM(i.quantidade * p.custo_unitario), 2)  AS cmv,
            ROUND(SUM(i.valor_total_item)
                  - SUM(i.quantidade * p.custo_unitario), 2) AS lucro_bruto,
            ROUND(
                (SUM(i.valor_total_item) - SUM(i.quantidade * p.custo_unitario))
                / NULLIF(SUM(i.valor_total_item), 0) * 100,
            1)                                              AS margem_pct
        FROM fct_itens_pedido_venda i
        JOIN dim_produtos p ON i.cod_produto = p.cod_produto
        JOIN fct_pedidos_venda v ON i.num_pedido = v.num_pedido
        WHERE v.status_pedido NOT IN ('CANCELADO', 'DEVOLVIDO')
        GROUP BY p.cod_produto
        ORDER BY receita_bruta DESC
    """).fetchall()
    return [dict(r) for r in rows]
