# ATLAS — Automated Transaction Loading & Analytics System

> **ERP Data Pipeline** | Python · SQLite · Pandas · SQL
> <img width="1881" height="856" alt="image" src="https://github.com/user-attachments/assets/8f5d2fdd-30c5-43ff-9c53-b575d381f1e8" />
<img width="1862" height="853" alt="image" src="https://github.com/user-attachments/assets/be939127-d57f-4fe9-9069-1957ff80130f" />
<img width="1840" height="821" alt="image" src="https://github.com/user-attachments/assets/5f091c04-09b1-4cf6-8df7-ef780b078ef7" />
<img width="1853" height="829" alt="image" src="https://github.com/user-attachments/assets/0c0809c5-5ad7-4449-938b-3cc6a5c62a65" />
<img width="1899" height="784" alt="image" src="https://github.com/user-attachments/assets/17ea1c5c-2053-41b2-a910-276a5d32bb31" />
<img width="1859" height="871" alt="image" src="https://github.com/user-attachments/assets/d0e3f1aa-f796-4235-a45f-413e2df67792" />


[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://python.org)
[![SQLite](https://img.shields.io/badge/SQLite-3.x-lightgrey.svg)](https://sqlite.org)
[![Pandas](https://img.shields.io/badge/Pandas-2.x-150458.svg)](https://pandas.pydata.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
<img width="1824" height="846" alt="image" src="https://github.com/user-attachments/assets/b65d37b6-7255-4345-bd1e-04d96306dadf" />

---

## Visão Geral

**ATLAS** é um pipeline ETL profissional que simula o processamento de dados de um ERP corporativo. O sistema ingere dados brutos de movimentações de estoque, pedidos de venda, pedidos de compra e cadastros mestres, aplica validações e regras de negócio, e carrega os dados tratados em um Data Warehouse SQLite modelado em esquema estrela (Star Schema).

O projeto foi construído com foco em demonstrar competências reais de **Data Engineering**:

| Competência | Como está demonstrado |
|---|---|
| **Ingestão** | `CSVReader` com detecção de schema, encoding e metadados de rastreio |
| **Transformação** | Transformers com Template Method pattern, tipagem, normalização |
| **Validação** | Regras de negócio por entidade + framework DQ genérico |
| **Carga** | Loader com staging → operational, upsert, batch, transações ACID |
| **Modelagem** | Star Schema: 4 dims + 5 fatos + 3 views analíticas |
| **SQL Analítico** | 7 queries gerenciais/operacionais com CTEs, window functions |
| **Qualidade de Dados** | DQ checks parametrizados, relatório consolidado, log de rejeições |
| **Observabilidade** | Logging estruturado (JSON) + tabelas de controle ETL no banco |
| **Testes** | 20+ unit tests com pytest cobrindo transformers e validators |
| **Organização** | Arquitetura em camadas, separação de responsabilidades, config central |

---

## Arquitetura

```
                        ┌──────────────────────────────────────────────┐
                        │            RAW DATA (CSV)                    │
                        │  produtos · clientes · fornecedores          │
                        │  pedidos_venda · pedidos_compra              │
                        │  movimentacoes_estoque                       │
                        └─────────────────┬────────────────────────────┘
                                          │
                               ┌──────────▼──────────┐
                               │  EXTRACT LAYER       │
                               │  CSVReader           │
                               │  • schema validation │
                               │  • metadata tagging  │
                               │  • encoding detect   │
                               └──────────┬───────────┘
                                          │
                    ┌─────────────────────▼──────────────────────┐
                    │          TRANSFORM LAYER                    │
                    │  BaseTransformer (Template Method)          │
                    │  ├── ProdutosTransformer                    │
                    │  ├── ClientesTransformer                    │
                    │  ├── PedidosVendaTransformer                │
                    │  ├── ItensPedidoVendaTransformer            │
                    │  ├── PedidosCompraTransformer               │
                    │  ├── ItensPedidoCompraTransformer           │
                    │  └── EstoqueTransformer                     │
                    │                                             │
                    │  DataQualityValidator → QualityReport       │
                    └──────────────┬──────────────────────────────┘
                                   │
              ┌────────────────────▼───────────────────────────────┐
              │             LOAD LAYER (SQLite)                    │
              │                                                    │
              │  ┌──────────────────────────────────────────────┐  │
              │  │  STAGING (full refresh per run)              │  │
              │  │  stg_produtos · stg_clientes · stg_pedidos…  │  │
              │  └──────────────────────────────────────────────┘  │
              │                                                    │
              │  ┌──────────────────────────────────────────────┐  │
              │  │  DIMENSIONAL (Star Schema)                   │  │
              │  │                                              │  │
              │  │  dim_produtos    dim_clientes                │  │
              │  │  dim_fornecedores  dim_tempo                 │  │
              │  │                                              │  │
              │  │  fct_pedidos_venda  fct_itens_pedido_venda   │  │
              │  │  fct_pedidos_compra fct_itens_pedido_compra  │  │
              │  │  fct_movimentacoes_estoque                   │  │
              │  └──────────────────────────────────────────────┘  │
              │                                                    │
              │  ┌──────────────────────────────────────────────┐  │
              │  │  CONTROL TABLES (Observabilidade)            │  │
              │  │  log_etl_execucoes · log_registros_rejeitados│  │
              │  └──────────────────────────────────────────────┘  │
              └────────────────────────────────────────────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │  ANALYTICS LAYER            │
                    │  7 SQL queries (views+CTEs) │
                    │  AnalyticsRunner CLI        │
                    └─────────────────────────────┘
```

---

## Estrutura do Projeto

```
atlas-erp-pipeline/
│
├── main.py                          # Entrypoint principal (CLI)
├── requirements.txt
├── .gitignore
│
├── config/
│   ├── settings.py                  # Paths, constantes, domínios válidos
│   └── logging_config.py            # Logging estruturado (JSON + console)
│
├── data/
│   ├── raw/                         # CSVs de entrada (8 arquivos)
│   │   ├── produtos.csv             # 25 produtos com dados intencionalmente sujos
│   │   ├── clientes.csv             # 17 clientes (CNPJs ausentes/malformados)
│   │   ├── fornecedores.csv
│   │   ├── pedidos_venda.csv        # 30 pedidos (cliente inválido, status inválido)
│   │   ├── itens_pedido_venda.csv   # 65 itens (preço negativo, produto inexistente)
│   │   ├── pedidos_compra.csv
│   │   ├── itens_pedido_compra.csv
│   │   └── movimentacoes_estoque.csv # 100 movs (data com formato diferente)
│   ├── processed/                   # Dados válidos exportados (gerado)
│   └── rejected/                    # Registros rejeitados com motivo (gerado)
│
├── database/
│   ├── schema.sql                   # DDL completo: staging + dims + fatos + views
│   └── erp_warehouse.db            # Banco gerado pelo pipeline (não versionado)
│
├── etl/
│   ├── extract/
│   │   └── csv_reader.py            # CSVReader com metadata, hash, validação de schema
│   ├── transform/
│   │   ├── base_transformer.py      # Template Method: parse → cast → dedup → rules
│   │   ├── produtos_transformer.py
│   │   ├── clientes_transformer.py  # Limpeza + validação de CNPJ
│   │   ├── pedidos_transformer.py   # 4 transformers (PV, itens PV, OC, itens OC)
│   │   └── estoque_transformer.py   # qty_sinal signed, validação de tipo_mov
│   ├── load/
│   │   └── sqlite_loader.py         # Context manager, upsert, batch, log_execucoes
│   └── pipeline.py                  # Orquestrador completo com métricas
│
├── quality/
│   ├── validators.py                # DataQualityValidator: null/dup/domain/range checks
│   └── quality_report.py           # Tabela formatada de resultados DQ
│
├── queries/
│   ├── operacional/
│   │   ├── estoque_atual.sql        # Posição de estoque com alertas CRITICO/ALERTA/OK
│   │   ├── pedidos_em_aberto.sql    # Aging de pedidos, detecção de atrasos
│   │   └── produtos_abaixo_minimo.sql # Custo de reposição por produto
│   └── gerencial/
│       ├── vendas_por_periodo.sql   # Receita mensal com variação MoM (window func)
│       ├── top_clientes.sql         # Pareto/ABC com share acumulado (window func)
│       ├── giro_estoque.sql         # Turnover e dias de cobertura
│       └── margem_por_produto.sql   # Receita, CMV, margem bruta %
│
├── reports/
│   └── analytics_runner.py         # CLI para executar queries e imprimir resultados
│
├── tests/
│   ├── test_transformers.py         # 18 unit tests de transformers
│   └── test_validators.py           # 7 unit tests de DQ validators
│
└── logs/
    └── atlas_pipeline.log          # Gerado em runtime (JSON + human-readable)
```

---

## Modelo de Dados (Star Schema)

```
                    ┌──────────────┐
                    │  dim_tempo   │
                    │  data_key PK │
                    └──────┬───────┘
                           │
         ┌─────────────────┼──────────────────┐
         │                 │                  │
┌────────▼──────┐  ┌───────▼────────┐  ┌─────▼──────────────┐
│  dim_clientes │  │  dim_produtos  │  │  dim_fornecedores  │
│  cod_cliente  │  │  cod_produto   │  │  cod_fornecedor    │
│  PK           │  │  PK            │  │  PK                │
└───────┬───────┘  └───────┬────────┘  └─────┬──────────────┘
        │                  │                  │
┌───────▼──────────────────▼──────────────────▼────────────┐
│                    FACT TABLES                            │
│                                                           │
│  fct_pedidos_venda   ←→   fct_itens_pedido_venda         │
│  fct_pedidos_compra  ←→   fct_itens_pedido_compra        │
│  fct_movimentacoes_estoque                                │
└───────────────────────────────────────────────────────────┘
```

**Derived Views**
- `vw_estoque_atual` — posição de estoque com status (CRITICO / ALERTA / OK)
- `vw_vendas_consolidadas` — pedidos + itens + clientes + tempo em uma view
- `vw_compras_consolidadas` — ordens de compra consolidadas

---

## Qualidade de Dados — Problemas Injetados

Os CSVs contêm dados sujos intencionais para demonstrar o pipeline de limpeza:

| Arquivo | Problema Injetado | Tratamento |
|---|---|---|
| `clientes.csv` | CNPJ ausente (CLI016), limite_credito nulo (CLI017) | Rejeição por CNPJ ausente; limite preenchido com 0 |
| `pedidos_venda.csv` | CLI999 (cliente inexistente) | Rejeição por FK inválida |
| `itens_pedido_venda.csv` | PRD999 (produto inexistente), preço = -50 | Rejeição por preço ≤ 0 |
| `movimentacoes_estoque.csv` | Data `01/05/2024` em vez de `2024-05-01` | Parse com formato alternativo |

---

## Como Executar

### 1. Pré-requisitos

```bash
python --version  # Python 3.11+
```

### 2. Clonar e instalar

```bash
git clone https://github.com/seu-usuario/atlas-erp-pipeline.git
cd atlas-erp-pipeline

python -m venv .venv
source .venv/bin/activate         # Linux/Mac
.venv\Scripts\activate            # Windows

pip install -r requirements.txt
```

### 3. Executar o pipeline completo

```bash
python main.py
```

Saída esperada:
```
╔══════════════════════════════╗
║  ATLAS ERP Data Pipeline     ║
╚══════════════════════════════╝

2024-05-01 10:00:01 | INFO     | atlas.pipeline       | Pipeline started | run_id=20240501_100001_a1b2c3
2024-05-01 10:00:01 | INFO     | atlas.extract        | Reading file: produtos.csv
2024-05-01 10:00:01 | INFO     | atlas.extract        | Extracted 25 rows | 14 columns | file=produtos.csv
...

════════════════════════════════════════════════════════════════════════
  ATLAS ERP PIPELINE — DATA QUALITY REPORT
════════════════════════════════════════════════════════════════════════
  Entity                          Rows   Checks    PASS    WARN    FAIL  Status
────────────────────────────────────────────────────────────────────────
  produtos                          25       16      16       0       0  [✓] PASS
  clientes                          17       12       9       2       1  [✗] FAIL
  ...
════════════════════════════════════════════════════════════════════════

  Done in 1.84s  |  Logs: logs/atlas_pipeline.log
```

### 4. Executar apenas as queries analíticas

```bash
python -m reports.analytics_runner
python -m reports.analytics_runner --query top_clientes
python -m reports.analytics_runner --query vendas_por_periodo
```

### 5. Executar os testes

```bash
python -m pytest tests/ -v
```

---

## Queries Analíticas Incluídas

### Operacional

| Query | Descrição |
|---|---|
| `estoque_atual` | Saldo atual por produto com classificação CRITICO / ALERTA / OK |
| `pedidos_em_aberto` | Pedidos pendentes com aging e flag de atraso |
| `produtos_abaixo_minimo` | Alerta de reposição com custo estimado |

### Gerencial

| Query | Técnicas SQL |
|---|---|
| `vendas_por_periodo` | `LAG()` window function para variação MoM |
| `top_clientes` | `SUM() OVER()` para Pareto/ABC com share acumulado |
| `giro_estoque` | Cálculo de turnover e dias de cobertura |
| `margem_por_produto` | Receita vs CMV, margem bruta % |

---

## Observabilidade

Cada execução do pipeline gera:

1. **Log estruturado** em `logs/atlas_pipeline.log`:
   ```json
   {"time": "2024-05-01T10:00:01", "level": "INFO", "logger": "atlas.pipeline", "message": "Loaded 25 rows → dim_produtos | mode=upsert | 0.03s"}
   ```

2. **Tabela `log_etl_execucoes`** no banco:
   ```sql
   SELECT * FROM log_etl_execucoes ORDER BY executed_at DESC;
   ```

3. **Tabela `log_registros_rejeitados`** com dados rejeitados + motivo

4. **Arquivos CSV de rejeição** em `data/rejected/` com timestamp

---

## Roadmap — Evolução para Produção

### Fase 1 — Orquestração com Apache Airflow
- [ ] Converter `ERPPipeline.run()` em `PythonOperator` tasks
- [ ] Criar DAG `atlas_erp_daily` com schedule `@daily`
- [ ] Adicionar sensores de arquivo (`FileSensor`) para aguardar os CSVs
- [ ] Implementar `on_failure_callback` com alertas por e-mail

### Fase 2 — Escala e Cloud
- [ ] Substituir SQLite por **PostgreSQL** (ou BigQuery/Snowflake)
- [ ] Migrar ingestão para **Apache Kafka** ou **AWS S3 + Lambda**
- [ ] Implementar **Slowly Changing Dimensions (SCD Type 2)** nas dimensões
- [ ] Adicionar **dbt** para transformações e documentação do modelo

### Fase 3 — Qualidade e Observabilidade
- [ ] Integrar **Great Expectations** para contratos de dados
- [ ] Adicionar **OpenTelemetry** para tracing distribuído
- [ ] Dashboard de qualidade com **Apache Superset** ou **Metabase**
- [ ] Alertas proativos de anomalia com **Z-score** nas métricas

### Fase 4 — Features Avançadas
- [ ] Processamento incremental (CDC — Change Data Capture)
- [ ] Histórico de preços e custos (SCD)
- [ ] Previsão de demanda com **Prophet** ou **statsmodels**
- [ ] API REST para consultas analíticas (FastAPI)

---

## Decisões Técnicas

| Decisão | Alternativa Considerada | Motivo da Escolha |
|---|---|---|
| SQLite como warehouse | PostgreSQL, DuckDB | Zero setup, portável, suficiente para demo |
| Template Method nos transformers | Strategy pattern | Garante pipeline uniforme com override cirúrgico |
| Todos os CSVs lidos como `str` | Tipagem automática do Pandas | Controle total sobre parsing e mensagens de erro precisas |
| WAL mode no SQLite | Padrão (DELETE) | Melhor performance em escrita concorrente |
| `INSERT OR REPLACE` para upsert | `ON CONFLICT DO UPDATE` | Compatibilidade com SQLite 3.x |

---

## Tecnologias

- **Python 3.11+** — linguagem principal
- **Pandas 2.x** — manipulação e transformação de dados
- **SQLite 3** — warehouse local (via módulo `sqlite3` nativo)
- **pytest** — testes unitários
- **logging** (stdlib) — logging estruturado

---

## Contribuindo

1. Fork o projeto
2. Crie sua branch: `git checkout -b feature/nova-fonte`
3. Commit: `git commit -m "feat: add XML ingestion layer"`
4. Push: `git push origin feature/nova-fonte`
5. Abra um Pull Request

---

## Licença

MIT © 2024

---

*Construído como projeto de competências em Data Engineering.*
