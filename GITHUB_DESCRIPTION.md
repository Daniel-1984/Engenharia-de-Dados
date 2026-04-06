# GitHub Repository Description

## Short Description (for "About" field — 1 line)
Professional ERP Data Pipeline: ETL with Python, Pandas & SQLite — Star Schema, DQ validation, structured logging, 28 unit tests.

## Topics / Tags
```
data-engineering  etl  python  sqlite  pandas  sql  star-schema
data-quality  elt-pipeline  erp  data-warehouse  portfolio
```

## GitHub Social Preview Text
ATLAS — Automated Transaction Loading & Analytics System
A production-grade ERP data pipeline simulating real-world scenarios: inventory management, purchase orders, sales orders, and product/customer master data. Built with Python, Pandas, and SQLite.

---

# LinkedIn Post

---

Acabei de finalizar o **ATLAS**, meu projeto de portfólio de Engenharia de Dados — e estou animado para compartilhar.

**O que é?**
Um pipeline ETL profissional que simula o processamento de dados de um ERP corporativo: movimentações de estoque, pedidos de venda, ordens de compra e cadastros mestres.

**O que foi construído:**

**Arquitetura em camadas** (Extract → Transform → Load):
- `CSVReader` com detecção de schema, hash de linha e metadados de rastreio
- 6 Transformers com Template Method Pattern e validação de regras de negócio
- `SQLiteLoader` com upsert, batch loading e transações ACID

**Modelagem Data Warehouse (Star Schema):**
- 4 tabelas de dimensão (produtos, clientes, fornecedores, tempo)
- 5 tabelas fato (pedidos de venda/compra, itens, movimentações de estoque)
- 3 views analíticas para consultas simplificadas

**Framework de Qualidade de Dados:**
- Validações parametrizadas: nulos, duplicatas, domínio, integridade referencial
- Relatório consolidado por entidade com classificação PASS/WARN/FAIL
- Registros rejeitados logados com motivo (arquivo CSV + tabela de controle)

**Observabilidade:**
- Logging estruturado em JSON (arquivo rotativo)
- Tabela `log_etl_execucoes` com métricas por execução
- run_id para rastreio end-to-end de cada carga

**SQL Analítico:**
- 7 queries com CTEs e Window Functions
- Ranking ABC de clientes com share acumulado (`SUM() OVER`)
- Variação MoM de receita com `LAG()`
- Giro de estoque e dias de cobertura

**28 testes unitários** com pytest — transformers e validators

**Roadmap documentado** para evolução com Apache Airflow, dbt e Great Expectations

---

Construído com: **Python · Pandas · SQLite · SQL · pytest**

Link no GitHub: [seu_usuario/atlas-erp-pipeline]

---

#DataEngineering #Python #SQL #ETL #DataWarehouse #Portfolio #OpenToWork
