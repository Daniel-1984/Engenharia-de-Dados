-- =============================================================================
-- ATLAS — Operational Query: Open Sales Orders
-- Lists all orders not yet delivered or cancelled, with aging.
-- =============================================================================

SELECT
    pv.num_pedido,
    pv.data_pedido,
    pv.data_entrega_prevista,
    pv.status,
    c.razao_social          AS cliente,
    c.estado,
    COUNT(iv.seq_item)      AS qtd_itens,
    ROUND(SUM(iv.valor_total_item) * (1 - pv.desconto_percentual / 100), 2)
                            AS valor_liquido,
    CAST(JULIANDAY('now') - JULIANDAY(pv.data_pedido) AS INTEGER)
                            AS dias_em_aberto,
    CASE
        WHEN pv.data_entrega_prevista < DATE('now') THEN 'ATRASADO'
        ELSE 'NO_PRAZO'
    END                     AS situacao_entrega
FROM fct_pedidos_venda pv
JOIN dim_clientes            c  ON pv.cod_cliente = c.cod_cliente
JOIN fct_itens_pedido_venda  iv ON pv.num_pedido  = iv.num_pedido
WHERE pv.status NOT IN ('ENTREGUE', 'CANCELADO', 'DEVOLVIDO')
GROUP BY
    pv.num_pedido, pv.data_pedido, pv.data_entrega_prevista,
    pv.status, c.razao_social, c.estado, pv.desconto_percentual
ORDER BY pv.data_entrega_prevista;
