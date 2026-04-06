-- =============================================================================
-- ATLAS — Operational Query: Products Below Minimum Stock
-- Critical replenishment alert.
-- =============================================================================

SELECT
    e.cod_produto,
    e.descricao,
    e.categoria,
    e.unidade,
    e.saldo_atual,
    e.estoque_minimo,
    e.estoque_minimo - e.saldo_atual    AS deficit,
    e.custo_unitario,
    ROUND((e.estoque_minimo - e.saldo_atual) * e.custo_unitario, 2)
                                        AS custo_reposicao_minimo,
    f.razao_social                      AS fornecedor_principal,
    f.prazo_entrega_dias
FROM vw_estoque_atual e
JOIN dim_produtos    p ON e.cod_produto = p.cod_produto
JOIN dim_fornecedores f ON p.cod_fornecedor_principal = f.cod_fornecedor
WHERE e.saldo_atual <= e.estoque_minimo
  AND e.status_estoque IN ('CRITICO', 'ALERTA')
ORDER BY deficit DESC;
