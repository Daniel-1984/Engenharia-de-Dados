# Como Rodar o Projeto ATLAS

## Passo 1 — Instalar dependencias (so na primeira vez)

```bash
cd "atlas-erp-pipeline"
py -m pip install pandas pytest rich streamlit plotly
```

## Passo 2 — Rodar o pipeline ETL

```bash
py main.py
```

## Passo 3 — Abrir o Dashboard web

```bash
py -m streamlit run dashboard.py
```

Abra no navegador: http://localhost:8501

## Rodar os testes

```bash
py -m pytest tests/ -v
```
