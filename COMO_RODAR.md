# Como Rodar o Projeto ATLAS

## Passo 1 — Instalar dependencias (so na primeira vez)

```bash
cd "atlas-erp-pipeline"
py -m pip install -r requirements.txt
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

## Passo 4 — Rodar a API REST (Fase 4 do Roadmap)

```bash
py -m uvicorn api.main:app --reload
```

Abra no navegador:
- http://localhost:8000/docs       (Swagger — documentacao interativa)
- http://localhost:8000/redoc      (ReDoc)
- http://localhost:8000/health     (status da API)
- http://localhost:8000/estoque/   (exemplo de endpoint)

## Rodar os testes

```bash
py -m pytest tests/ -v
```

---

## Roadmap — O que roda gratuitamente no seu computador

| Fase | Ferramenta        | Como instalar                        | Para que serve                        |
|------|-------------------|--------------------------------------|---------------------------------------|
| 1    | Apache Airflow    | `pip install apache-airflow`         | Agendar pipeline todo dia automatico  |
| 1    | FileSensor        | incluido no Airflow                  | Aguardar CSVs chegarem antes de rodar |
| 2    | PostgreSQL        | https://www.postgresql.org/download  | Banco mais robusto que SQLite         |
| 2    | dbt Core          | `pip install dbt-core dbt-sqlite`    | Transformacoes SQL com documentacao   |
| 3    | Great Expectations| `pip install great-expectations`     | Contratos de qualidade de dados       |
| 4    | FastAPI           | `pip install fastapi uvicorn`        | API REST para consultas analiticas    |
| 4    | Prophet           | `pip install prophet`                | Previsao de demanda com sazonalidade  |

## Como usar o Airflow (Fase 1)

```bash
# Instalar
pip install apache-airflow

# Inicializar banco interno do Airflow
airflow db init

# Criar usuario admin
airflow users create --username admin --password admin \
  --firstname Atlas --lastname Admin --role Admin --email admin@atlas.com

# Copiar a DAG para o diretorio do Airflow
cp airflow/dags/atlas_erp_dag.py ~/airflow/dags/

# Rodar em dois terminais separados:
airflow webserver --port 8080   # UI: http://localhost:8080
airflow scheduler
```
