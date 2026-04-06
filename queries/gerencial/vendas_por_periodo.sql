-- =============================================================================
-- ATLAS — Managerial Query: Sales Revenue by Period
-- Monthly & quarterly revenue with MoM and YoY growth.
-- =============================================================================

WITH monthly AS (
    SELECT
        t.ano,
        t.mes,
        t.nome_mes,
        t.trimestre,
        COUNT(DISTINCT vc.num_pedido)   AS qtd_pedidos,
        COUNT(DISTINCT vc.cod_cliente)  AS clientes_unicos,
        ROUND(SUM(vc.valor_liquido), 2) AS receita_liquida,
        ROUND(AVG(vc.valor_liquido), 2) AS ticket_medio
    FROM vw_vendas_consolidadas vc
    JOIN dim_tempo t ON t.data = vc.data_pedido
    GROUP BY t.ano, t.mes, t.nome_mes, t.trimestre
),
with_lag AS (
    SELECT
        *,
        LAG(receita_liquida) OVER (ORDER BY ano, mes) AS receita_mes_anterior
    FROM monthly
)
SELECT
    ano,
    mes,
    nome_mes,
    trimestre,
    qtd_pedidos,
    clientes_unicos,
    receita_liquida,
    ticket_medio,
    ROUND(
        (receita_liquida - receita_mes_anterior) / NULLIF(receita_mes_anterior, 0) * 100, 1
    ) AS variacao_mom_pct
FROM with_lag
ORDER BY ano, mes;
