-- =============================================================================
-- ATLAS ERP Pipeline — Database Schema
-- SQLite Warehouse
-- =============================================================================

PRAGMA journal_mode = WAL;
-- Note: foreign_keys is managed by the loader connection, not here.

-- =============================================================================
-- STAGING LAYER  (raw ingested data — full refresh per run)
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg_produtos (
    cod_produto             TEXT,
    descricao               TEXT,
    categoria               TEXT,
    unidade                 TEXT,
    custo_unitario          TEXT,
    preco_venda             TEXT,
    estoque_minimo          TEXT,
    estoque_maximo          TEXT,
    ativo                   TEXT,
    cod_fornecedor_principal TEXT,
    _source_file            TEXT,
    _ingestion_ts           TEXT,
    _pipeline_run_id        TEXT,
    _row_hash               TEXT
);

CREATE TABLE IF NOT EXISTS stg_clientes (
    cod_cliente     TEXT,
    razao_social    TEXT,
    cnpj            TEXT,
    segmento        TEXT,
    cidade          TEXT,
    estado          TEXT,
    limite_credito  TEXT,
    ativo           TEXT,
    data_cadastro   TEXT,
    _source_file     TEXT,
    _ingestion_ts    TEXT,
    _pipeline_run_id TEXT,
    _row_hash        TEXT
);

CREATE TABLE IF NOT EXISTS stg_fornecedores (
    cod_fornecedor      TEXT,
    razao_social        TEXT,
    cnpj                TEXT,
    categoria           TEXT,
    cidade              TEXT,
    estado              TEXT,
    prazo_entrega_dias  TEXT,
    ativo               TEXT,
    _source_file        TEXT,
    _ingestion_ts       TEXT,
    _pipeline_run_id    TEXT,
    _row_hash           TEXT
);

CREATE TABLE IF NOT EXISTS stg_pedidos_venda (
    num_pedido              TEXT,
    cod_cliente             TEXT,
    data_pedido             TEXT,
    data_entrega_prevista   TEXT,
    data_entrega_real       TEXT,
    status                  TEXT,
    desconto_percentual     TEXT,
    observacao              TEXT,
    _source_file            TEXT,
    _ingestion_ts           TEXT,
    _pipeline_run_id        TEXT,
    _row_hash               TEXT
);

CREATE TABLE IF NOT EXISTS stg_itens_pedido_venda (
    num_pedido              TEXT,
    seq_item                TEXT,
    cod_produto             TEXT,
    quantidade              TEXT,
    preco_unitario          TEXT,
    desconto_item_percentual TEXT,
    _source_file            TEXT,
    _ingestion_ts           TEXT,
    _pipeline_run_id        TEXT,
    _row_hash               TEXT
);

CREATE TABLE IF NOT EXISTS stg_pedidos_compra (
    num_oc                  TEXT,
    cod_fornecedor          TEXT,
    data_oc                 TEXT,
    data_entrega_prevista   TEXT,
    data_recebimento        TEXT,
    status                  TEXT,
    observacao              TEXT,
    _source_file            TEXT,
    _ingestion_ts           TEXT,
    _pipeline_run_id        TEXT,
    _row_hash               TEXT
);

CREATE TABLE IF NOT EXISTS stg_itens_pedido_compra (
    num_oc          TEXT,
    seq_item        TEXT,
    cod_produto     TEXT,
    quantidade      TEXT,
    custo_unitario  TEXT,
    _source_file    TEXT,
    _ingestion_ts   TEXT,
    _pipeline_run_id TEXT,
    _row_hash       TEXT
);

CREATE TABLE IF NOT EXISTS stg_movimentacoes_estoque (
    num_mov         TEXT,
    data_mov        TEXT,
    tipo_mov        TEXT,
    cod_produto     TEXT,
    quantidade      TEXT,
    valor_unitario  TEXT,
    origem          TEXT,
    num_documento   TEXT,
    observacao      TEXT,
    _source_file    TEXT,
    _ingestion_ts   TEXT,
    _pipeline_run_id TEXT,
    _row_hash       TEXT
);

-- =============================================================================
-- DIMENSIONAL LAYER
-- =============================================================================

CREATE TABLE IF NOT EXISTS dim_produtos (
    cod_produto              TEXT PRIMARY KEY,
    descricao                TEXT NOT NULL,
    categoria                TEXT,
    unidade                  TEXT,
    custo_unitario           REAL,
    preco_venda              REAL,
    margem_bruta             REAL GENERATED ALWAYS AS (
                               ROUND((preco_venda - custo_unitario) / preco_venda * 100, 2)
                             ) VIRTUAL,
    estoque_minimo           INTEGER DEFAULT 0,
    estoque_maximo           INTEGER DEFAULT 0,
    ativo                    INTEGER DEFAULT 1,
    cod_fornecedor_principal TEXT,
    _dw_loaded_at            TEXT,
    _is_active               INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS dim_clientes (
    cod_cliente     TEXT PRIMARY KEY,
    razao_social    TEXT NOT NULL,
    cnpj            TEXT,
    segmento        TEXT,
    cidade          TEXT,
    estado          TEXT,
    limite_credito  REAL DEFAULT 0,
    ativo           INTEGER DEFAULT 1,
    data_cadastro   TEXT,
    _dw_loaded_at   TEXT,
    _is_active      INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS dim_fornecedores (
    cod_fornecedor      TEXT PRIMARY KEY,
    razao_social        TEXT NOT NULL,
    cnpj                TEXT,
    categoria           TEXT,
    cidade              TEXT,
    estado              TEXT,
    prazo_entrega_dias  INTEGER,
    ativo               INTEGER DEFAULT 1,
    _dw_loaded_at       TEXT,
    _is_active          INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS dim_tempo (
    data_key        INTEGER PRIMARY KEY,   -- YYYYMMDD
    data            TEXT NOT NULL,
    ano             INTEGER,
    semestre        INTEGER,
    trimestre       INTEGER,
    mes             INTEGER,
    nome_mes        TEXT,
    semana_ano      INTEGER,
    dia             INTEGER,
    dia_semana      INTEGER,
    nome_dia        TEXT,
    is_fim_semana   INTEGER DEFAULT 0,
    _dw_loaded_at   TEXT,
    _is_active      INTEGER DEFAULT 1
);

-- =============================================================================
-- FACT LAYER
-- =============================================================================

CREATE TABLE IF NOT EXISTS fct_pedidos_venda (
    num_pedido              TEXT PRIMARY KEY,
    cod_cliente             TEXT,
    data_pedido             TEXT,
    data_entrega_prevista   TEXT,
    data_entrega_real       TEXT,
    status                  TEXT,
    desconto_percentual     REAL DEFAULT 0,
    observacao              TEXT,
    _dw_loaded_at           TEXT,
    _is_active              INTEGER DEFAULT 1,
    FOREIGN KEY (cod_cliente) REFERENCES dim_clientes(cod_cliente)
);

CREATE TABLE IF NOT EXISTS fct_itens_pedido_venda (
    num_pedido               TEXT,
    seq_item                 INTEGER,
    cod_produto              TEXT,
    quantidade               REAL,
    preco_unitario           REAL,
    desconto_item_percentual REAL DEFAULT 0,
    valor_total_item         REAL,
    _dw_loaded_at            TEXT,
    _is_active               INTEGER DEFAULT 1,
    PRIMARY KEY (num_pedido, seq_item),
    FOREIGN KEY (num_pedido)  REFERENCES fct_pedidos_venda(num_pedido),
    FOREIGN KEY (cod_produto) REFERENCES dim_produtos(cod_produto)
);

CREATE TABLE IF NOT EXISTS fct_pedidos_compra (
    num_oc                  TEXT PRIMARY KEY,
    cod_fornecedor          TEXT,
    data_oc                 TEXT,
    data_entrega_prevista   TEXT,
    data_recebimento        TEXT,
    status                  TEXT,
    observacao              TEXT,
    _dw_loaded_at           TEXT,
    _is_active              INTEGER DEFAULT 1,
    FOREIGN KEY (cod_fornecedor) REFERENCES dim_fornecedores(cod_fornecedor)
);

CREATE TABLE IF NOT EXISTS fct_itens_pedido_compra (
    num_oc          TEXT,
    seq_item        INTEGER,
    cod_produto     TEXT,
    quantidade      REAL,
    custo_unitario  REAL,
    valor_total_item REAL,
    _dw_loaded_at   TEXT,
    _is_active      INTEGER DEFAULT 1,
    PRIMARY KEY (num_oc, seq_item),
    FOREIGN KEY (num_oc)      REFERENCES fct_pedidos_compra(num_oc),
    FOREIGN KEY (cod_produto) REFERENCES dim_produtos(cod_produto)
);

CREATE TABLE IF NOT EXISTS fct_movimentacoes_estoque (
    num_mov         TEXT PRIMARY KEY,
    data_mov        TEXT,
    tipo_mov        TEXT,
    cod_produto     TEXT,
    quantidade      REAL,
    qty_sinal       REAL,          -- signed: +ENTRADA / -SAIDA
    valor_unitario  REAL,
    valor_total_mov REAL,
    origem          TEXT,
    num_documento   TEXT,
    observacao      TEXT,
    _dw_loaded_at   TEXT,
    _is_active      INTEGER DEFAULT 1,
    FOREIGN KEY (cod_produto) REFERENCES dim_produtos(cod_produto)
);

-- =============================================================================
-- CONTROL / OBSERVABILITY TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS log_etl_execucoes (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_run_id  TEXT NOT NULL,
    entity           TEXT NOT NULL,
    status           TEXT NOT NULL,
    input_rows       INTEGER,
    valid_rows       INTEGER,
    rejected_rows    INTEGER,
    elapsed_seconds  REAL,
    error_message    TEXT,
    executed_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS log_registros_rejeitados (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    entity           TEXT NOT NULL,
    reject_reason    TEXT,
    row_data         TEXT,
    pipeline_run     TEXT,
    logged_at        TEXT NOT NULL
);

-- =============================================================================
-- ANALYTICAL VIEWS
-- =============================================================================

CREATE VIEW IF NOT EXISTS vw_estoque_atual AS
SELECT
    p.cod_produto,
    p.descricao,
    p.categoria,
    p.unidade,
    p.custo_unitario,
    p.preco_venda,
    p.estoque_minimo,
    p.estoque_maximo,
    COALESCE(SUM(m.qty_sinal), 0)                         AS saldo_atual,
    COALESCE(SUM(m.qty_sinal) * p.custo_unitario, 0)      AS valor_estoque,
    CASE
        WHEN COALESCE(SUM(m.qty_sinal), 0) <= p.estoque_minimo THEN 'CRITICO'
        WHEN COALESCE(SUM(m.qty_sinal), 0) <= p.estoque_minimo * 1.5 THEN 'ALERTA'
        ELSE 'OK'
    END AS status_estoque
FROM dim_produtos p
LEFT JOIN fct_movimentacoes_estoque m ON p.cod_produto = m.cod_produto
WHERE p._is_active = 1
GROUP BY p.cod_produto, p.descricao, p.categoria, p.unidade,
         p.custo_unitario, p.preco_venda, p.estoque_minimo, p.estoque_maximo;


CREATE VIEW IF NOT EXISTS vw_vendas_consolidadas AS
SELECT
    pv.num_pedido,
    pv.data_pedido,
    pv.status,
    pv.cod_cliente,
    c.razao_social      AS cliente,
    c.segmento,
    c.estado,
    COUNT(iv.seq_item)  AS qtd_itens,
    SUM(iv.valor_total_item) AS valor_bruto,
    pv.desconto_percentual,
    ROUND(SUM(iv.valor_total_item) * (1 - pv.desconto_percentual / 100), 2) AS valor_liquido,
    t.mes,
    t.trimestre,
    t.ano
FROM fct_pedidos_venda pv
JOIN dim_clientes       c  ON pv.cod_cliente = c.cod_cliente
JOIN fct_itens_pedido_venda iv ON pv.num_pedido = iv.num_pedido
LEFT JOIN dim_tempo     t  ON REPLACE(pv.data_pedido, '-', '') = CAST(t.data_key AS TEXT)
WHERE pv.status != 'CANCELADO'
GROUP BY pv.num_pedido, pv.data_pedido, pv.status, pv.cod_cliente,
         c.razao_social, c.segmento, c.estado, pv.desconto_percentual,
         t.mes, t.trimestre, t.ano;


CREATE VIEW IF NOT EXISTS vw_compras_consolidadas AS
SELECT
    pc.num_oc,
    pc.data_oc,
    pc.status,
    pc.cod_fornecedor,
    f.razao_social      AS fornecedor,
    f.categoria         AS categoria_fornecedor,
    COUNT(ic.seq_item)  AS qtd_itens,
    SUM(ic.valor_total_item) AS valor_total_oc,
    t.mes,
    t.trimestre,
    t.ano
FROM fct_pedidos_compra pc
JOIN dim_fornecedores    f  ON pc.cod_fornecedor = f.cod_fornecedor
JOIN fct_itens_pedido_compra ic ON pc.num_oc = ic.num_oc
LEFT JOIN dim_tempo      t  ON REPLACE(pc.data_oc, '-', '') = CAST(t.data_key AS TEXT)
WHERE pc.status != 'CANCELADO'
GROUP BY pc.num_oc, pc.data_oc, pc.status, pc.cod_fornecedor,
         f.razao_social, f.categoria, t.mes, t.trimestre, t.ano;


CREATE INDEX IF NOT EXISTS idx_fct_vendas_cliente   ON fct_pedidos_venda(cod_cliente);
CREATE INDEX IF NOT EXISTS idx_fct_vendas_data      ON fct_pedidos_venda(data_pedido);
CREATE INDEX IF NOT EXISTS idx_fct_itens_v_pedido   ON fct_itens_pedido_venda(num_pedido);
CREATE INDEX IF NOT EXISTS idx_fct_itens_v_produto  ON fct_itens_pedido_venda(cod_produto);
CREATE INDEX IF NOT EXISTS idx_fct_compras_forn     ON fct_pedidos_compra(cod_fornecedor);
CREATE INDEX IF NOT EXISTS idx_fct_estoque_produto  ON fct_movimentacoes_estoque(cod_produto);
CREATE INDEX IF NOT EXISTS idx_fct_estoque_data     ON fct_movimentacoes_estoque(data_mov);
CREATE INDEX IF NOT EXISTS idx_log_etl_run          ON log_etl_execucoes(pipeline_run_id);
