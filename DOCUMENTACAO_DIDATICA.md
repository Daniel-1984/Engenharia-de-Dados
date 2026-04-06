# ATLAS — Documentacao Didatica Completa
> Entenda cada parte do projeto como se eu fosse seu professor de Engenharia de Dados

---

## O que e Engenharia de Dados?

Engenharia de Dados e a area que constroi os "encanamentos" que levam dados de um lugar para outro. Assim como um engenheiro civil constroi estradas para que carros possam chegar ao destino, o Engenheiro de Dados constroi pipelines para que os dados cheguem onde precisam estar.

```
[FONTE DOS DADOS] --> [PIPELINE ETL] --> [BANCO DE DADOS] --> [DASHBOARD / RELATORIOS]
```

---

## Por que simular um ERP?

**ERP (Enterprise Resource Planning)** e o sistema que uma empresa usa para gerenciar:
- Estoque de produtos
- Pedidos de venda (o que o cliente comprou)
- Pedidos de compra (o que a empresa comprou do fornecedor)
- Cadastro de produtos, clientes e fornecedores

**Empresas reais** como Totvs Protheus, SAP, Oracle usam ERP. Esses sistemas geram enormes volumes de dados que precisam ser processados. Por isso, saber trabalhar com dados de ERP e uma habilidade muito valorizada no mercado.

---

## O que e um Pipeline ETL?

**ETL = Extract, Transform, Load**

| Letra | Ingles  | Portugues      | O que faz                                      |
|-------|---------|----------------|------------------------------------------------|
| E     | Extract | Extrair        | Le os dados de onde estao (CSV, banco, API)    |
| T     | Transform | Transformar  | Limpa, valida e prepara os dados               |
| L     | Load    | Carregar       | Salva os dados tratados no destino final       |

No ATLAS:
- **Extract** = `etl/extract/csv_reader.py` le os arquivos CSV
- **Transform** = `etl/transform/` limpa e valida cada tipo de dado
- **Load** = `etl/load/sqlite_loader.py` salva no banco SQLite

---

## De onde vem os dados?

### No mundo real:
Os dados viriam do proprio sistema ERP da empresa. Exemplos:
- O ERP exporta um arquivo CSV todo dia com as vendas do dia
- Uma API REST do ERP retorna os pedidos em JSON
- O banco do ERP tem uma tabela que e lida diretamente

### Neste projeto:
Criamos arquivos CSV que simulam esse cenario. Sao dados **ficticios mas realistas**, com:
- Empresas brasileiras com CNPJ
- Produtos de informatica com preco e custo reais
- Pedidos com datas, status, descontos
- Movimentacoes de estoque com entradas e saidas

### Onde ficam os CSVs?
```
data/raw/
  produtos.csv              -- 25 produtos cadastrados
  clientes.csv              -- 17 clientes (com CNPJ, segmento, limite de credito)
  fornecedores.csv          -- 8 fornecedores
  pedidos_venda.csv         -- 30 pedidos de venda
  itens_pedido_venda.csv    -- 67 itens de pedidos de venda
  pedidos_compra.csv        -- 20 ordens de compra
  itens_pedido_compra.csv   -- 42 itens de ordens de compra
  movimentacoes_estoque.csv -- 100 movimentacoes de entrada/saida
```

---

## Por que os dados tem "sujeira"?

No mundo real, dados nunca chegam perfeitos. Sempre tem problemas:

| Problema real              | Como simulamos aqui                          |
|----------------------------|----------------------------------------------|
| CNPJ nao preenchido        | CLI016 esta sem CNPJ no arquivo              |
| Preco com erro de digitacao| Item do pedido PV-2024-030 tem preco = -50   |
| Data em formato errado     | MOV-0097 tem data como "01/05/2024" (dd/mm)  |
| Cliente que nao existe     | Pedido PV-2024-030 tem CLI999 (nao cadastrado)|
| Produto que nao existe     | Item PRD999 referenciado em um pedido         |

O pipeline detecta esses problemas, **rejeita** os registros ruins e salva em `data/rejected/` com o motivo.

---

## O que e Qualidade de Dados (DQ)?

**Data Quality = Qualidade dos dados**

Antes de salvar, verificamos se os dados sao confiaveis:

| Regra                 | Exemplo                                       |
|-----------------------|-----------------------------------------------|
| Nao pode ser nulo     | Todo produto precisa ter um codigo            |
| Valor valido          | Preco de venda deve ser maior que zero        |
| Formato correto       | CNPJ deve ter 14 digitos                      |
| Logica de negocio     | Data de entrega nao pode ser antes do pedido  |
| Sem duplicatas        | Nao pode ter dois produtos com o mesmo codigo |

O framework DQ esta em `quality/validators.py`. Ele gera um relatorio em `quality/quality_report.py`.

---

## O que e um Data Warehouse?

**Data Warehouse** = Armazem de dados. Um banco de dados **especial** para analise, diferente do banco operacional do dia a dia.

### Banco Operacional (OLTP)
- Otimizado para **escrever** dados rapido
- Muitas tabelas com poucas colunas
- Usado pelo sistema ERP no dia a dia

### Data Warehouse (OLAP)
- Otimizado para **consultar** e **analisar** dados
- Modelo estrela (Star Schema)
- Usado por analistas e gestores para tomar decisoes

---

## O que e Star Schema (Esquema Estrela)?

E um modelo de dados para DW que se parece com uma estrela:

```
                  [dim_tempo]
                       |
[dim_clientes] -- [TABELA FATO] -- [dim_produtos]
                       |
                [dim_fornecedores]
```

- **Dimensoes (dim_)**: Quem, O que, Quando, Onde. Exemplo: dim_produtos tem nome, categoria, preco.
- **Fatos (fct_)**: O que aconteceu, os numeros. Exemplo: fct_pedidos_venda tem valor, quantidade, data.

### No ATLAS temos:
| Dimensoes          | Fatos                         |
|--------------------|-------------------------------|
| dim_produtos       | fct_pedidos_venda             |
| dim_clientes       | fct_itens_pedido_venda        |
| dim_fornecedores   | fct_pedidos_compra            |
| dim_tempo          | fct_itens_pedido_compra       |
|                    | fct_movimentacoes_estoque     |

---

## Cada arquivo Python — o que ele faz?

### config/settings.py
Centraliza todas as **configuracoes** do projeto. Caminhos de arquivos, nome do banco, regras de negocio. Nao espalhamos configuracoes pelo codigo — tudo fica aqui.

```python
DATABASE_PATH = BASE_DIR / "database" / "erp_warehouse.db"
VALID_UNIDADES = {"UN", "CX", "RS", "LI", "KG"}
```

### config/logging_config.py
Configura o sistema de **logs**. O log e como um diario do pipeline — registra tudo que aconteceu, com horario e nivel (INFO, WARNING, ERROR).

```
2024-05-01 10:00:01 | INFO | Extraido 25 linhas de produtos.csv
2024-05-01 10:00:01 | WARNING | 1 linha rejeitada: CNPJ invalido
```

### etl/extract/csv_reader.py
Le um arquivo CSV e devolve um DataFrame Pandas. Tambem adiciona colunas de metadados:
- `_source_file`: de qual arquivo veio a linha
- `_ingestion_ts`: quando foi lida
- `_row_hash`: um "codigo unico" de cada linha (para detectar duplicatas entre execucoes)

### etl/transform/base_transformer.py
Classe base que define os **passos padrao** de transformacao (Template Method Pattern):
1. Padroniza nomes das colunas (remove espacos, lowercase)
2. Converte datas
3. Converte numeros
4. Remove duplicatas
5. Aplica regras de negocio (cada subclasse define as suas)

### etl/transform/produtos_transformer.py
Regras especificas de produtos:
- Custo deve ser > 0
- Preco deve ser > custo (senao esta vendendo com prejuizo)
- Unidade deve ser UN, CX, RS, etc.
- Descricao convertida para MAIUSCULO (padronizacao)

### etl/transform/clientes_transformer.py
- CNPJ limpo (remove pontos, barras, traco) e validado (14 digitos)
- CNPJ formatado de volta: "12.345.678/0001-90"
- Limite de credito nulo = 0 (regra de negocio)

### etl/load/sqlite_loader.py
Salva os dados no banco SQLite. Tem 3 modos:
- `replace`: apaga tudo e recarrega (usado para tabelas staging)
- `append`: adiciona sem apagar (nao usado aqui)
- `upsert`: insere ou substitui (garante que nao teremos duplicata por PK)

### etl/pipeline.py
O "maestro" que coordena tudo. Ele chama, em ordem:
1. CSVReader para extrair
2. DataQualityValidator para verificar qualidade
3. Transformer para limpar
4. SQLiteLoader para salvar na staging
5. SQLiteLoader para salvar na tabela operacional
6. Loga tudo em log_etl_execucoes

### quality/validators.py
Framework de qualidade de dados generico. Voce passa qualquer DataFrame e ele verifica:
- Colunas obrigatorias presentes?
- Campos com muitos nulos?
- Linhas duplicadas?

### database/schema.sql
O DDL (Data Definition Language) do banco. Define todas as tabelas, tipos, chaves primarias, indices e views. Equivale ao "planta baixa" do banco de dados.

### queries/
Queries SQL separadas por tipo:
- `operacional/`: para uso do dia a dia (o que esta em estoque? quais pedidos atrasados?)
- `gerencial/`: para gestores tomarem decisoes (qual cliente gera mais receita? qual produto tem mais margem?)

### dashboard.py
Interface web feita com Streamlit. Le o banco SQLite e exibe graficos e tabelas interativas no navegador.

---

## Conceitos de SQL usados nas queries

### CTE (Common Table Expression)
Consulta nomeada dentro de outra consulta. Deixa o SQL mais legivel:

```sql
WITH cliente_receita AS (
    SELECT cod_cliente, SUM(valor) AS total
    FROM pedidos GROUP BY cod_cliente
)
SELECT * FROM cliente_receita WHERE total > 1000;
```

### Window Function
Calculo sobre um grupo de linhas sem agrupar. Usada para rankings e variacao:

```sql
-- Receita acumulada (Pareto)
SUM(receita) OVER (ORDER BY receita DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- Variacao mes a mes (MoM)
LAG(receita) OVER (ORDER BY ano, mes)
```

### JOIN
Une duas tabelas pela chave em comum:
```sql
SELECT p.nome_produto, c.nome_cliente, v.valor
FROM vendas v
JOIN produtos p ON v.cod_produto = p.cod_produto
JOIN clientes c ON v.cod_cliente = c.cod_cliente
```

---

## Conceitos de Python usados

### Pandas DataFrame
Como uma planilha Excel dentro do Python. Cada linha e um registro, cada coluna e um campo.

```python
import pandas as pd
df = pd.read_csv("produtos.csv")
df[df["preco_venda"] > 0]        # filtrar
df["margem"] = df["preco"] - df["custo"]  # nova coluna
```

### Orientacao a Objetos (Classes)
Cada transformer e uma classe. Permite reutilizar codigo e ter comportamentos especificos:

```python
class ProdutosTransformer(BaseTransformer):
    def _apply_business_rules(self, df):
        # regras especificas de produtos
        ...
```

### Context Manager (with)
Garante que o banco sera fechado corretamente mesmo se ocorrer erro:

```python
with SQLiteLoader() as loader:
    loader.load(df, "dim_produtos")
# banco fechado automaticamente aqui
```

### Logging
Registra o que acontece durante a execucao sem poluir a tela do usuario:

```python
logger.info("Arquivo lido com sucesso: 25 linhas")
logger.warning("CNPJ invalido encontrado: CLI016")
logger.error("Arquivo nao encontrado!")
```

---

## Roadmap de evolucao

### Nivel 1 (este projeto)
- Python + Pandas + SQLite + SQL
- Pipeline local com CSVs
- Dashboard Streamlit

### Nivel 2 (proximo passo)
- **Apache Airflow**: agendar o pipeline para rodar todo dia automaticamente
- **PostgreSQL**: banco mais robusto para producao
- **dbt**: ferramenta para transformacoes SQL com documentacao automatica

### Nivel 3 (producao em nuvem)
- **AWS S3**: armazenar os arquivos na nuvem
- **AWS Glue / Databricks**: processar grandes volumes com Spark
- **Snowflake / BigQuery**: Data Warehouse em nuvem
- **Great Expectations**: contratos de qualidade de dados

---

## O que estudar depois deste projeto?

| Topico                  | Por que estudar                              | Recurso sugerido            |
|-------------------------|----------------------------------------------|-----------------------------|
| SQL avancado            | Base de tudo em Engenharia de Dados          | Mode Analytics SQL Tutorial |
| Apache Airflow          | Orquestrar pipelines reais                   | Documentacao oficial        |
| dbt                     | Transformacoes SQL com boas praticas         | dbt Learn                   |
| Apache Spark            | Processar volumes enormes de dados           | Databricks Community        |
| Data Modeling           | Desenhar Data Warehouses profissionais       | Kimball Group               |
| Docker                  | Empacotar aplicacoes para qualquer maquina  | Docker Getting Started      |

---

## Glossario rapido

| Termo             | Significado simples                                                |
|-------------------|--------------------------------------------------------------------|
| Pipeline          | Sequencia de etapas que processa dados de ponta a ponta            |
| ETL               | Extract (ler) > Transform (limpar) > Load (salvar)                 |
| Data Warehouse    | Banco de dados especial para analise e relatorios                  |
| Star Schema       | Modelo de dados com tabelas fato (numeros) e dimensao (contexto)   |
| Data Quality      | Conjunto de regras que garantem que os dados sao confiaveis        |
| Staging           | Camada temporaria que guarda os dados brutos antes de limpar       |
| Upsert            | Insere se nao existe, atualiza se ja existe                        |
| CNPJ              | Cadastro Nacional de Pessoa Juridica (14 digitos)                  |
| Window Function   | Calculo SQL sobre um conjunto de linhas sem agrupar               |
| CTE               | "Consulta com nome" que facilita leitura de SQL complexo           |
| run_id            | Identificador unico de cada execucao do pipeline (rastreabilidade) |
| Log               | Registro cronologico do que aconteceu durante a execucao           |
| DataFrame         | Estrutura de dados Pandas (como uma tabela/planilha no Python)     |
