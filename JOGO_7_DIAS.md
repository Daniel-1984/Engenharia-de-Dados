# ATLAS ACADEMY — Aprenda Engenharia de Dados em 7 Dias
> Cada dia é uma fase. Cada fase tem missões. Complete tudo e você vira Engenheiro de Dados.

---

```
NIVEL DE DIFICULDADE:  [■■□□□] Iniciante -> [■■■■■] Engenheiro
TEMPO POR DIA:         45 minutos a 1 hora
FERRAMENTAS:           Python, SQLite, o próprio projeto ATLAS
REGRA:                 Nao pule fases. Cada dia depende do anterior.
```

---

## FASE 1 — "O Mapa do Tesouro"
### Tema: Entender o projeto sem escrever uma linha de código
**XP disponível: 100 pontos**

### Por que esta fase existe?
Todo engenheiro de dados antes de mexer em qualquer coisa **lê e entende** o que já existe.
Isso se chama **engenharia reversa** — descobrir como algo funciona desmontando ele.

---

### MISSAO 1.1 — Explorar a estrutura (15 min)
Abra o terminal e rode:
```bash
cd atlas-erp-pipeline
py main.py
```
Observe a saída. Responda no papel:
- Quantas entidades foram processadas?
- Quantas linhas foram rejeitadas e por quê?
- Quanto tempo levou o pipeline?

**+20 XP se você conseguir responder as 3 perguntas**

---

### MISSAO 1.2 — Ler os dados brutos (15 min)
Abra os arquivos em `data/raw/` com Excel ou qualquer editor.
Responda:
- Qual produto tem o maior preço de venda?
- Qual cliente não tem CNPJ preenchido?
- Qual movimentação tem a data no formato errado?

**Dica:** as respostas estão na `DOCUMENTACAO_DIDATICA.md`

**+20 XP**

---

### MISSAO 1.3 — O Mapa da Arquitetura (10 min)
Desenhe no papel (sim, no papel!) o fluxo:
```
[   ?   ]  -->  [   ?   ]  -->  [   ?   ]  -->  [   ?   ]
```
Preencha os 4 quadrados. Use o README.md como referência.

**+30 XP**

---

### CHEFE DA FASE 1 — Quiz
Responda sem olhar:
1. O que significa ETL?
2. Qual arquivo Python é o "maestro" do pipeline?
3. O que é um Data Warehouse?
4. Qual a diferença entre uma tabela `dim_` e uma `fct_`?

**+30 XP (7,5 por resposta certa)**

---

### RECOMPENSA DA FASE 1
```
[CONQUISTA DESBLOQUEADA]: "Detetive de Dados"
Você sabe o que o projeto faz antes de tocar no código.
Isso é raro. A maioria dos iniciantes sai escrevendo sem entender.
```

---

## FASE 2 — "As Fontes do Rio"
### Tema: De onde vêm os dados — CSV, Pandas e Extração
**XP disponível: 150 pontos**

### Por que esta fase existe?
Dados precisam **vir de algum lugar**. No mundo real vêm de ERPs, APIs, bancos.
Aqui vêm de CSVs. O módulo `etl/extract/csv_reader.py` faz essa leitura.

---

### MISSAO 2.1 — Ler um CSV com Python (20 min)
Crie um arquivo `pratica/dia2.py`:
```python
import pandas as pd

# Leia o arquivo de produtos
df = pd.read_csv("data/raw/produtos.csv")

# Missão: responda com código
print("Total de produtos:", len(df))
print("Colunas:", df.columns.tolist())
print("Produtos com preco > 1000:")
print(df[df["preco_venda"] > 1000][["cod_produto", "descricao", "preco_venda"]])
```
Rode: `py pratica/dia2.py`

**+30 XP**

---

### MISSAO 2.2 — Engenharia reversa do csv_reader.py (20 min)
Abra `etl/extract/csv_reader.py` e responda:
1. O que faz o parâmetro `dtype=str`? Por que não ler já com o tipo correto?
2. O que é `_row_hash`? Para que serve?
3. O que acontece se o arquivo não existir?

**Dica:** leia o código linha por linha. É menor do que parece.

**+40 XP**

---

### MISSAO 2.3 — Modificar o leitor (20 min)
No arquivo `pratica/dia2.py`, adicione:
```python
# Adicione uma coluna calculada
df["margem_bruta"] = df["preco_venda"].astype(float) - df["custo_unitario"].astype(float)
df["margem_pct"] = (df["margem_bruta"] / df["preco_venda"].astype(float) * 100).round(1)

# Qual produto tem maior margem percentual?
print(df.sort_values("margem_pct", ascending=False)[["descricao", "margem_pct"]].head(5))
```

**+40 XP**

---

### CHEFE DA FASE 2
Crie `pratica/dia2_chefe.py` que:
1. Leia `clientes.csv`
2. Filtre só os clientes do segmento "INDUSTRIA"
3. Mostre o nome e o limite de crédito
4. Calcule a média do limite de crédito desse segmento

**+40 XP**

---

### RECOMPENSA DA FASE 2
```
[CONQUISTA DESBLOQUEADA]: "Extrator"
Você sabe ler dados de qualquer CSV com Python.
O E do ETL está dominado.
```

---

## FASE 3 — "O Filtro de Ouro"
### Tema: Qualidade de Dados e Transformação
**XP disponível: 200 pontos**

### Por que esta fase existe?
Dados nunca chegam limpos. Um Engenheiro de Dados passa **60-70% do tempo** limpando dados.
Essa fase ensina a detectar e rejeitar dados ruins — o coração do projeto ATLAS.

---

### MISSAO 3.1 — Entender os rejeitos (15 min)
Rode o pipeline e depois abra `data/rejected/`:
```bash
py main.py --skip-analytics
```
Abra cada CSV rejeitado e anote:
- Qual entidade gerou mais rejeições?
- Qual foi o motivo mais comum?
- Esses problemas existiriam em uma empresa real?

**+30 XP**

---

### MISSAO 3.2 — Engenharia reversa do validator (20 min)
Abra `quality/validators.py`. Identifique:
1. Quais são os 3 tipos de verificação que o validator faz?
2. O que acontece se uma coluna obrigatória estiver faltando?
3. Como o validator decide se um campo tem "muitos nulos"?

**+40 XP**

---

### MISSAO 3.3 — Escrever sua própria validação (30 min)
Crie `pratica/dia3.py`:
```python
import pandas as pd

df = pd.read_csv("data/raw/produtos.csv")
df["preco_venda"] = pd.to_numeric(df["preco_venda"], errors="coerce")
df["custo_unitario"] = pd.to_numeric(df["custo_unitario"], errors="coerce")

# Escreva regras de validação:
# Regra 1: preco_venda deve ser maior que zero
invalidos_preco = df[df["preco_venda"] <= 0]
print(f"Produtos com preco invalido: {len(invalidos_preco)}")

# MISSAO: escreva mais 2 regras voce mesmo
# Regra 2: custo_unitario deve ser menor que preco_venda
# Regra 3: estoque_atual nao pode ser negativo
# ... (complete você mesmo)
```

**+50 XP**

---

### CHEFE DA FASE 3
Abra `etl/transform/clientes_transformer.py`.
Escreva com suas próprias palavras (em português, sem código):
1. O que a função `_clean_cnpj` faz passo a passo?
2. Por que remover os caracteres `.`, `/`, `-` do CNPJ?
3. Por que formatar de volta para `XX.XXX.XXX/XXXX-XX`?

Depois valide: abra `data/raw/clientes.csv`, pegue qualquer CNPJ
e simule o processo manualmente no papel.

**+80 XP**

---

### RECOMPENSA DA FASE 3
```
[CONQUISTA DESBLOQUEADA]: "Guardião da Qualidade"
Você sabe por que dados sujos destroem análises.
O T do ETL está dominado.
```

---

## FASE 4 — "O Cofre"
### Tema: Banco de dados, SQL e o modelo Star Schema
**XP disponível: 250 pontos**

### Por que esta fase existe?
SQL é a linguagem mais importante em Engenharia de Dados.
Sem SQL você não analisa nada. Esta fase é a mais importante do jogo.

---

### MISSAO 4.1 — Explorar o banco (20 min)
Instale o DB Browser for SQLite (gratuito): https://sqlitebrowser.org/
Abra `database/erp_warehouse.db`.

Explore e responda:
- Quantas tabelas existem?
- Qual a diferença visual entre tabelas `dim_` e `fct_`?
- Quais tabelas têm mais linhas?

**+30 XP**

---

### MISSAO 4.2 — Primeiras queries (30 min)
No DB Browser, aba "Execute SQL", rode cada query e anote o resultado:

```sql
-- Query 1: quantos produtos por categoria?
SELECT categoria, COUNT(*) as total
FROM dim_produtos
GROUP BY categoria
ORDER BY total DESC;

-- Query 2: qual cliente tem mais pedidos?
SELECT cod_cliente, COUNT(*) as pedidos
FROM fct_pedidos_venda
GROUP BY cod_cliente
ORDER BY pedidos DESC
LIMIT 5;

-- Query 3: receita total por mês
SELECT strftime('%Y-%m', data_pedido) as mes,
       ROUND(SUM(valor_total), 2) as receita
FROM fct_pedidos_venda
WHERE status_pedido NOT IN ('CANCELADO', 'DEVOLVIDO')
GROUP BY mes
ORDER BY mes;
```

**+60 XP**

---

### MISSAO 4.3 — Engenharia reversa do schema (20 min)
Abra `database/schema.sql`. Encontre e explique:
1. O que é `PRIMARY KEY`?
2. O que é `NOT NULL`?
3. O que faz a view `vw_estoque_atual`?
4. O que é um `INDEX` e por que criamos um?

**+60 XP**

---

### CHEFE DA FASE 4 — Escreva do zero
Escreva uma query SQL que responda:
**"Quais são os 5 produtos mais vendidos (em quantidade) que ainda estão com estoque abaixo do mínimo?"**

Dicas:
- Você precisará de `JOIN` entre `fct_itens_pedido_venda` e `vw_estoque_atual`
- Use `GROUP BY` para somar quantidades
- Use `WHERE` para filtrar os que estão abaixo do mínimo

**+100 XP**

---

### RECOMPENSA DA FASE 4
```
[CONQUISTA DESBLOQUEADA]: "Arquiteto do Cofre"
Você entende SQL e modelos de dados.
O L do ETL está dominado. Pipeline completo!
```

---

## FASE 5 — "O Painel de Controle"
### Tema: Dashboard, visualização e Streamlit
**XP disponível: 200 pontos**

### Por que esta fase existe?
Dados no banco não valem nada se ninguém consegue enxergar.
O dashboard transforma números em decisões.

---

### MISSAO 5.1 — Explorar o dashboard (15 min)
Rode:
```bash
py -m streamlit run dashboard.py
```
Explore todas as 5 abas. Responda:
- Qual aba mostra os produtos com estoque crítico?
- Onde aparecem as anomalias de qualidade de dados?
- Qual cliente tem maior receita?

**+30 XP**

---

### MISSAO 5.2 — Engenharia reversa do dashboard (20 min)
Abra `dashboard.py`. Encontre no código:
1. A linha que busca dados do banco (função `query()`)
2. Como um gráfico de barras é criado com Plotly
3. Como o `@st.cache_data` funciona e por que é importante

**+40 XP**

---

### MISSAO 5.3 — Adicionar um gráfico novo (30 min)
No final da aba "Vendas" do dashboard, adicione um novo gráfico:
```python
# Cole isso dentro do bloco "with tab_vendas:"
st.subheader("Distribuicao de Descontos")
fig_desc = px.histogram(
    query("SELECT desconto_pct FROM fct_pedidos_venda WHERE desconto_pct > 0"),
    x="desconto_pct",
    title="Frequencia de descontos concedidos",
    labels={"desconto_pct": "Desconto (%)"},
    nbins=10,
)
fig_desc.update_layout(plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="white")
st.plotly_chart(fig_desc, use_container_width=True)
```

**+130 XP**

---

### RECOMPENSA DA FASE 5
```
[CONQUISTA DESBLOQUEADA]: "Narrador de Dados"
Você transforma dados em histórias visuais.
Gestores vão te adorar.
```

---

## FASE 6 — "A Torre de Controle"
### Tema: API REST, FastAPI e como sistemas se comunicam
**XP disponível: 200 pontos**

### Por que esta fase existe?
No mundo real, dados não ficam só em dashboards.
Outros sistemas (apps, ferramentas de BI, scripts) precisam **consultar dados via API**.
A API é a ponte entre o Data Warehouse e o mundo.

---

### MISSAO 6.1 — Explorar a API (15 min)
Rode:
```bash
py -m uvicorn api.main:app --reload
```
Abra http://localhost:8000/docs

Clique em cada endpoint e teste o botão "Try it out".
Responda:
- O que retorna `/estoque/criticos`?
- O que retorna `/vendas/kpis`?
- O que retorna `/qualidade/anomalias`?

**+30 XP**

---

### MISSAO 6.2 — Engenharia reversa da API (20 min)
Abra `api/routers/vendas.py`. Responda:
1. O que faz o `Depends(get_db)`?
2. Por que usamos `Query(None, description=...)` nos parâmetros?
3. O que acontece se o banco não existir? (dica: veja `api/database.py`)

**+50 XP**

---

### MISSAO 6.3 — Criar um endpoint novo (30 min)
Em `api/routers/estoque.py`, adicione:
```python
@router.get("/valor-total", summary="Valor total do estoque")
def get_valor_total(conn: sqlite3.Connection = Depends(get_db)):
    """Retorna o valor total investido em estoque."""
    row = conn.execute("""
        SELECT
            ROUND(SUM(valor_estoque), 2) AS valor_total,
            COUNT(*) AS total_produtos
        FROM vw_estoque_atual
    """).fetchone()
    return dict(row)
```
Teste em http://localhost:8000/docs

**+120 XP**

---

### RECOMPENSA DA FASE 6
```
[CONQUISTA DESBLOQUEADA]: "Engenheiro de APIs"
Você sabe criar endpoints REST profissionais.
Sistemas inteiros dependem do que você constrói.
```

---

## FASE 7 — "O Dia do Chefe Final"
### Tema: Construir algo do zero — projeto próprio mini
**XP disponível: 500 pontos**

### Por que esta fase existe?
Aprender fazendo é o único jeito que funciona.
Esta fase você vai construir um mini-pipeline do zero, sem copiar.

---

### MISSAO FINAL — Mini Pipeline de Vendas de Livros

**Contexto:** Uma livraria tem 3 arquivos CSV simples.
Sua missão: construir um pipeline ETL completo para eles.

**Passo 1 — Crie os dados** (`pratica/livros/`)
```
livros.csv:
cod_livro,titulo,autor,preco,genero
LIV001,O Hobbit,Tolkien,49.90,Fantasia
LIV002,1984,Orwell,35.00,Distopia
LIV003,Dom Casmurro,Machado,29.90,Romance
LIV004,Duna,Herbert,-10.00,Ficcao  <- ERRO intencional
LIV005,Neuromancer,Gibson,42.00,Ficcao

vendas_livros.csv:
cod_venda,cod_livro,quantidade,data_venda
V001,LIV001,2,2024-01-15
V002,LIV002,1,2024-01-16
V003,LIV999,3,2024-01-17  <- ERRO: livro nao existe
V004,LIV003,5,2024-02-01
V005,LIV004,1,2024-02-10
```

**Passo 2 — Escreva o ETL** (`pratica/livros/pipeline.py`)
Seu pipeline deve:
- [ ] Ler os CSVs com pandas
- [ ] Rejeitar livros com preço negativo
- [ ] Rejeitar vendas com cod_livro inválido
- [ ] Calcular valor_total (quantidade * preço)
- [ ] Salvar em SQLite (`pratica/livros/livros.db`)
- [ ] Imprimir um resumo no terminal

**Passo 3 — Escreva uma query** (`pratica/livros/relatorio.py`)
- Qual livro gerou mais receita?
- Qual gênero vendeu mais unidades?

**Passo 4 — (BONUS +100 XP)** Crie um endpoint FastAPI
`GET /livros/mais-vendidos` que retorna o ranking.

---

### PONTUACAO DO CHEFE FINAL
| Critério | XP |
|---|---|
| ETL lê e processa os CSVs | +100 |
| Rejeições funcionando corretamente | +100 |
| valor_total calculado | +50 |
| Dados salvos no SQLite | +100 |
| Queries respondendo certo | +100 |
| Endpoint FastAPI funcionando | +100 (BONUS) |

---

## PLACAR FINAL

| Fase | Tema | XP Máximo |
|---|---|---|
| 1 | Mapa do Tesouro | 100 |
| 2 | Fontes do Rio | 150 |
| 3 | Filtro de Ouro | 200 |
| 4 | O Cofre (SQL) | 250 |
| 5 | Painel de Controle | 200 |
| 6 | Torre de Controle (API) | 200 |
| 7 | Chefe Final | 500 |
| **TOTAL** | | **1600 XP** |

### Ranking
| XP | Titulo |
|---|---|
| 0 - 400 | Aprendiz de Dados |
| 401 - 800 | Analista de Dados |
| 801 - 1200 | Engenheiro Jr |
| 1201 - 1500 | Engenheiro Pleno |
| 1501 - 1600 | Engenheiro Sênior |

---

## Dicas para não travar

**Se travar no código:** Abra o arquivo equivalente do ATLAS e use como referência.
O projeto inteiro é seu gabarito.

**Se não entender um conceito:** Leia a `DOCUMENTACAO_DIDATICA.md`.
Ela foi escrita exatamente para você nesse momento.

**Se um erro aparecer:** Cole o erro no Google ou no ChatGPT.
Saber pesquisar erros é 50% do trabalho de todo engenheiro.

**Regra de ouro:**
```
Nao copie. Leia, entenda, feche, e escreva de memoria.
O que voce escreve de memoria, voce nunca esquece.
```

---

## O que estudar depois dos 7 dias

| Semana | Topico | Recurso gratuito |
|---|---|---|
| 2 | SQL avancado (CTEs, Window Functions) | mode.com/sql-tutorial |
| 3 | Apache Airflow | airflow.apache.org |
| 4 | dbt Core | courses.getdbt.com (gratis) |
| 5 | Docker | docker.com/get-started |
| 6 | PostgreSQL | postgresqltutorial.com |
| 7 | Cloud (AWS Free Tier) | aws.amazon.com/free |

**Em 2 meses voce estara pronto para sua primeira vaga de Engenheiro de Dados Jr.**
