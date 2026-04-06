-- =============================================================================
-- ATLAS — Operational Query: Current Inventory Position
-- Returns current stock level, value, and status for all active products.
-- =============================================================================

SELECT
    cod_produto,
    descricao,
    categoria,
    unidade,
    saldo_atual,
    estoque_minimo,
    estoque_maximo,
    ROUND(valor_estoque, 2)  AS valor_estoque,
    custo_unitario,
    preco_venda,
    status_estoque
FROM vw_estoque_atual
ORDER BY
    CASE status_estoque
        WHEN 'CRITICO' THEN 1
        WHEN 'ALERTA'  THEN 2
        ELSE 3
    END,
    descricao;
