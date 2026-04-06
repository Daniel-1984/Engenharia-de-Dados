-- =============================================================================
-- ATLAS — Managerial Query: Top Customers by Revenue (Pareto / ABC)
-- Includes cumulative revenue share and ABC classification.
-- =============================================================================

WITH cliente_receita AS (
    SELECT
        cod_cliente,
        cliente,
        segmento,
        estado,
        COUNT(DISTINCT num_pedido)   AS qtd_pedidos,
        ROUND(SUM(valor_liquido), 2) AS receita_total,
        ROUND(AVG(valor_liquido), 2) AS ticket_medio,
        MIN(data_pedido)             AS primeiro_pedido,
        MAX(data_pedido)             AS ultimo_pedido
    FROM vw_vendas_consolidadas
    GROUP BY cod_cliente, cliente, segmento, estado
),
total AS (
    SELECT SUM(receita_total) AS receita_geral FROM cliente_receita
),
ranked AS (
    SELECT
        cr.*,
        t.receita_geral,
        ROUND(cr.receita_total / t.receita_geral * 100, 2)  AS share_pct,
        ROW_NUMBER() OVER (ORDER BY cr.receita_total DESC)   AS ranking,
        SUM(cr.receita_total) OVER (
            ORDER BY cr.receita_total DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS receita_acumulada
    FROM cliente_receita cr, total t
)
SELECT
    ranking,
    cod_cliente,
    cliente,
    segmento,
    estado,
    qtd_pedidos,
    receita_total,
    ticket_medio,
    share_pct,
    ROUND(receita_acumulada / receita_geral * 100, 1) AS share_acumulado_pct,
    CASE
        WHEN receita_acumulada / receita_geral <= 0.80 THEN 'A'
        WHEN receita_acumulada / receita_geral <= 0.95 THEN 'B'
        ELSE 'C'
    END AS classe_abc,
    primeiro_pedido,
    ultimo_pedido
FROM ranked
ORDER BY ranking;
