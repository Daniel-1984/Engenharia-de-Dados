-- =============================================================================
-- ATLAS — Managerial Query: Inventory Turnover (Giro de Estoque)
-- Measures how many times inventory is sold/replaced in a period.
-- =============================================================================

WITH saidas AS (
    SELECT
        cod_produto,
        SUM(ABS(qty_sinal))          AS total_saida_qtd,
        SUM(valor_total_mov)         AS cmv_periodo       -- Custo Mercadoria Vendida
    FROM fct_movimentacoes_estoque
    WHERE tipo_mov = 'SAIDA'
    GROUP BY cod_produto
),
estoque_medio AS (
    -- Approximation: (opening + closing) / 2
    SELECT
        cod_produto,
        (SUM(CASE WHEN tipo_mov = 'ENTRADA' THEN qty_sinal ELSE 0 END)
         + SUM(CASE WHEN tipo_mov = 'SAIDA'  THEN qty_sinal ELSE 0 END)) / 2.0
            AS estoque_medio_qtd
    FROM fct_movimentacoes_estoque
    GROUP BY cod_produto
)
SELECT
    p.cod_produto,
    p.descricao,
    p.categoria,
    p.custo_unitario,
    COALESCE(s.total_saida_qtd, 0)          AS total_saida_qtd,
    COALESCE(s.cmv_periodo, 0)              AS cmv_periodo,
    COALESCE(e.estoque_medio_qtd, 0)        AS estoque_medio_qtd,
    ROUND(
        COALESCE(s.total_saida_qtd, 0)
        / NULLIF(ABS(COALESCE(e.estoque_medio_qtd, 0)), 0),
        2
    )                                        AS giro_estoque,
    ROUND(
        365.0 / NULLIF(
            COALESCE(s.total_saida_qtd, 0)
            / NULLIF(ABS(COALESCE(e.estoque_medio_qtd, 0)), 0),
        0),
        0
    )                                        AS dias_cobertura
FROM dim_produtos p
LEFT JOIN saidas        s ON p.cod_produto = s.cod_produto
LEFT JOIN estoque_medio e ON p.cod_produto = e.cod_produto
WHERE p._is_active = 1
ORDER BY giro_estoque DESC NULLS LAST;
