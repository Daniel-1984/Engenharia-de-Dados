"""
ATLAS API — Previsao de demanda com media movel e tendencia linear
Usa apenas numpy/pandas — sem dependencia de Prophet para manter instalacao simples.
Para usar Prophet: pip install prophet  (instrucoes em /previsao/docs)
"""
import sqlite3
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, Query, HTTPException

from api.database import get_db

router = APIRouter(prefix="/previsao", tags=["Previsao de Demanda"])


def _tendencia_linear(y: list[float]) -> dict:
    """Ajusta uma reta y = a + b*x e retorna coeficientes."""
    n = len(y)
    if n < 2:
        return {"a": float(y[0]) if y else 0, "b": 0.0}
    x = np.arange(n, dtype=float)
    b = (n * np.dot(x, y) - x.sum() * sum(y)) / (n * np.dot(x, x) - x.sum() ** 2)
    a = (sum(y) - b * x.sum()) / n
    return {"a": round(float(a), 2), "b": round(float(b), 2)}


@router.get("/demanda", summary="Previsao de demanda — media movel + tendencia")
def get_previsao_demanda(
    cod_produto: Optional[str] = Query(None, description="Codigo do produto (ex: PRD001)"),
    meses_futuros: int = Query(3, ge=1, le=12, description="Quantos meses prever"),
    conn: sqlite3.Connection = Depends(get_db),
):
    """
    Previsao de demanda mensal usando **media movel de 3 periodos** + **tendencia linear**.

    - Historico: quantidade vendida por mes por produto
    - Previsao: projecao para os proximos N meses
    - Metodo: tendencia linear (regressao simples por minimos quadrados)

    Para previsao avancada com sazonalidade, integre Prophet:
    `pip install prophet`
    """
    sql = """
        SELECT
            strftime('%Y-%m', v.data_pedido) AS periodo,
            i.cod_produto,
            p.descricao,
            SUM(i.quantidade)                AS quantidade
        FROM fct_itens_pedido_venda i
        JOIN fct_pedidos_venda v ON i.num_pedido = v.num_pedido
        JOIN dim_produtos p ON i.cod_produto = p.cod_produto
        WHERE v.status_pedido NOT IN ('CANCELADO', 'DEVOLVIDO')
    """
    params: list = []
    if cod_produto:
        sql += " AND i.cod_produto = ?"
        params.append(cod_produto.upper())
    sql += " GROUP BY periodo, i.cod_produto ORDER BY i.cod_produto, periodo"

    rows = conn.execute(sql, params).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="Nenhum dado de venda encontrado")

    df = pd.DataFrame([dict(r) for r in rows])
    resultado = []

    for produto_id, grupo in df.groupby("cod_produto"):
        grupo = grupo.sort_values("periodo")
        historico = grupo[["periodo", "quantidade"]].to_dict("records")
        qtds = grupo["quantidade"].tolist()

        # Tendencia linear
        trend = _tendencia_linear(qtds)
        n = len(qtds)

        # Media movel ultimos 3 periodos como base
        base = float(np.mean(qtds[-3:])) if len(qtds) >= 3 else float(np.mean(qtds))

        # Gerar datas futuras
        ultimo_periodo = grupo["periodo"].iloc[-1]
        ano, mes = map(int, ultimo_periodo.split("-"))
        previsoes = []
        for i in range(1, meses_futuros + 1):
            mes += 1
            if mes > 12:
                mes = 1
                ano += 1
            # previsao = base + tendencia acumulada
            qtd_prevista = max(0, round(base + trend["b"] * (n + i - 1)))
            previsoes.append({
                "periodo": f"{ano:04d}-{mes:02d}",
                "quantidade_prevista": qtd_prevista,
                "intervalo_inferior": max(0, round(qtd_prevista * 0.8)),
                "intervalo_superior": round(qtd_prevista * 1.2),
            })

        resultado.append({
            "cod_produto": produto_id,
            "descricao": grupo["descricao"].iloc[0],
            "historico": historico,
            "tendencia": trend,
            "media_movel_3m": round(base, 1),
            "previsao": previsoes,
        })

    return resultado


@router.get("/reposicao", summary="Sugestao de reposicao com base na previsao")
def get_reposicao(
    meses_cobertura: int = Query(2, ge=1, le=6, description="Meses de cobertura desejados"),
    conn: sqlite3.Connection = Depends(get_db),
):
    """
    Sugere quantidade a comprar para cada produto critico,
    com base na demanda media dos ultimos 3 meses e no saldo atual.

    Logica: qtd_comprar = (demanda_media * meses_cobertura) - saldo_atual
    """
    rows = conn.execute("""
        WITH demanda AS (
            SELECT
                i.cod_produto,
                AVG(mensal.qty) AS demanda_media_mensal
            FROM (
                SELECT
                    i2.cod_produto,
                    strftime('%Y-%m', v.data_pedido) AS periodo,
                    SUM(i2.quantidade)               AS qty
                FROM fct_itens_pedido_venda i2
                JOIN fct_pedidos_venda v ON i2.num_pedido = v.num_pedido
                WHERE v.status_pedido NOT IN ('CANCELADO', 'DEVOLVIDO')
                  AND v.data_pedido >= date('now', '-3 months')
                GROUP BY i2.cod_produto, periodo
            ) mensal
            JOIN fct_itens_pedido_venda i ON i.cod_produto = mensal.cod_produto
            GROUP BY i.cod_produto
        )
        SELECT
            e.cod_produto,
            e.descricao,
            e.categoria,
            e.saldo_atual,
            e.estoque_minimo,
            e.status_estoque,
            ROUND(d.demanda_media_mensal, 1)         AS demanda_media_mensal,
            MAX(0, ROUND(
                d.demanda_media_mensal * ? - e.saldo_atual
            ))                                       AS qtd_sugerida_compra,
            ROUND(
                MAX(0, d.demanda_media_mensal * ? - e.saldo_atual)
                * p.custo_unitario, 2
            )                                        AS custo_estimado
        FROM vw_estoque_atual e
        LEFT JOIN demanda d ON e.cod_produto = d.cod_produto
        LEFT JOIN dim_produtos p ON e.cod_produto = p.cod_produto
        WHERE e.status_estoque IN ('CRITICO', 'ALERTA')
          AND d.demanda_media_mensal > 0
        ORDER BY e.status_estoque, qtd_sugerida_compra DESC
    """, (meses_cobertura, meses_cobertura)).fetchall()

    return {
        "meses_cobertura": meses_cobertura,
        "total_itens": len(rows),
        "custo_total_estimado": round(sum(r["custo_estimado"] or 0 for r in rows), 2),
        "sugestoes": [dict(r) for r in rows],
    }
