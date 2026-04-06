-- =============================================================================
-- ATLAS — Managerial Query: Gross Margin by Product
-- Revenue, CMV, gross margin and margin % per product.
-- =============================================================================

SELECT
    p.cod_produto,
    p.descricao,
    p.categoria,
    p.custo_unitario,
    p.preco_venda,
    p.margem_bruta                          AS margem_bruta_catalog_pct,
    SUM(iv.quantidade)                      AS qtd_vendida,
    ROUND(SUM(iv.valor_total_item), 2)      AS receita_bruta,
    ROUND(SUM(iv.quantidade * p.custo_unitario), 2)
                                            AS cmv,
    ROUND(
        SUM(iv.valor_total_item) - SUM(iv.quantidade * p.custo_unitario),
        2
    )                                       AS margem_bruta_valor,
    ROUND(
        (SUM(iv.valor_total_item) - SUM(iv.quantidade * p.custo_unitario))
        / NULLIF(SUM(iv.valor_total_item), 0) * 100,
        2
    )                                       AS margem_bruta_pct
FROM fct_itens_pedido_venda  iv
JOIN dim_produtos             p  ON iv.cod_produto = p.cod_produto
JOIN fct_pedidos_venda        pv ON iv.num_pedido  = pv.num_pedido
WHERE pv.status NOT IN ('CANCELADO', 'DEVOLVIDO')
GROUP BY
    p.cod_produto, p.descricao, p.categoria,
    p.custo_unitario, p.preco_venda, p.margem_bruta
ORDER BY margem_bruta_valor DESC;
