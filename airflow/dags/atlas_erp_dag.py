"""
ATLAS ERP Pipeline — Apache Airflow DAG
========================================
Fase 1 do Roadmap de Evolução para Produção.

Esta DAG orquestra o pipeline ETL do ATLAS ERP de forma automática,
executando diariamente às 06:00 (horário de Brasília).

Estrutura da DAG:
  [aguardar_csvs] --> [validar_qualidade] --> [executar_etl] --> [verificar_carga]
                                                  |
                                          [notificar_sucesso]
                                          [notificar_falha]  (em caso de erro)

Como instalar o Airflow (gratuito, local):
  pip install apache-airflow
  airflow db init
  airflow webserver --port 8080  (UI em http://localhost:8080)
  airflow scheduler

Copie esta DAG para: $AIRFLOW_HOME/dags/
"""
from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------
ATLAS_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DATA_DIR = ATLAS_DIR / "data" / "raw"

DEFAULT_ARGS = {
    "owner": "atlas-data-team",
    "depends_on_past": False,
    "email": ["data-team@atlas.com.br"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

ARQUIVOS_ESPERADOS = [
    "produtos.csv",
    "clientes.csv",
    "fornecedores.csv",
    "pedidos_venda.csv",
    "itens_pedido_venda.csv",
    "pedidos_compra.csv",
    "itens_pedido_compra.csv",
    "movimentacoes_estoque.csv",
]


# ---------------------------------------------------------------------------
# Funcoes das tasks
# ---------------------------------------------------------------------------

def _validar_arquivos(**context) -> str:
    """Valida que todos os CSVs esperados existem e nao estao vazios."""
    ausentes = []
    for nome in ARQUIVOS_ESPERADOS:
        path = RAW_DATA_DIR / nome
        if not path.exists():
            ausentes.append(nome)
        elif path.stat().st_size == 0:
            ausentes.append(f"{nome} (vazio)")

    if ausentes:
        raise FileNotFoundError(f"Arquivos ausentes ou vazios: {ausentes}")

    print(f"[OK] Todos os {len(ARQUIVOS_ESPERADOS)} arquivos validados.")
    return "executar_etl"


def _executar_etl(**context) -> dict:
    """
    Executa o pipeline ETL principal.
    Equivale a rodar: py main.py --skip-analytics
    """
    result = subprocess.run(
        [sys.executable, str(ATLAS_DIR / "main.py"), "--skip-analytics"],
        capture_output=True,
        text=True,
        cwd=str(ATLAS_DIR),
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Pipeline falhou com codigo {result.returncode}:\n{result.stderr}"
        )

    print(result.stdout)
    # Empurra metricas para o XCom (compartilhamento entre tasks no Airflow)
    context["ti"].xcom_push(key="etl_stdout", value=result.stdout[:4000])
    return {"status": "success", "returncode": result.returncode}


def _verificar_carga(**context) -> None:
    """Verifica se o banco foi populado corretamente após o ETL."""
    import sqlite3

    db_path = ATLAS_DIR / "database" / "erp_warehouse.db"
    if not db_path.exists():
        raise FileNotFoundError("Banco de dados nao foi criado pelo ETL!")

    with sqlite3.connect(db_path) as conn:
        tabelas = {
            "dim_produtos": "SELECT COUNT(*) FROM dim_produtos",
            "dim_clientes": "SELECT COUNT(*) FROM dim_clientes",
            "fct_pedidos_venda": "SELECT COUNT(*) FROM fct_pedidos_venda",
            "fct_movimentacoes_estoque": "SELECT COUNT(*) FROM fct_movimentacoes_estoque",
        }
        for tabela, sql in tabelas.items():
            count = conn.execute(sql).fetchone()[0]
            if count == 0:
                raise ValueError(f"Tabela {tabela} esta vazia apos a carga!")
            print(f"  {tabela}: {count} registros")

    print("[OK] Carga verificada com sucesso.")


def _branch_resultado(**context) -> str:
    """Decide o caminho baseado no resultado do ETL."""
    ti = context["ti"]
    etl_result = ti.xcom_pull(task_ids="executar_etl")
    if etl_result and etl_result.get("status") == "success":
        return "notificar_sucesso"
    return "notificar_falha"


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="atlas_erp_daily",
    description="Pipeline ETL do ATLAS ERP — execucao diaria automatica",
    schedule="0 6 * * *",           # Todo dia as 06:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,              # Evita execucoes paralelas
    default_args=DEFAULT_ARGS,
    tags=["atlas", "erp", "etl", "producao"],
    doc_md=__doc__,
) as dag:

    # ------------------------------------------------------------------
    # Task 1: Sensores de arquivo — aguarda os CSVs chegarem
    # (simula um SFTP ou S3 que deposita os arquivos)
    # ------------------------------------------------------------------
    sensores = []
    for arquivo in ARQUIVOS_ESPERADOS:
        sensor = FileSensor(
            task_id=f"aguardar_{arquivo.replace('.csv', '')}",
            filepath=str(RAW_DATA_DIR / arquivo),
            poke_interval=60,        # verifica a cada 60s
            timeout=3600,            # timeout de 1h
            mode="reschedule",       # libera o worker enquanto aguarda
            fs_conn_id="fs_default",
        )
        sensores.append(sensor)

    # ------------------------------------------------------------------
    # Task 2: Valida arquivos
    # ------------------------------------------------------------------
    validar = PythonOperator(
        task_id="validar_arquivos",
        python_callable=_validar_arquivos,
    )

    # ------------------------------------------------------------------
    # Task 3: Executa o pipeline ETL
    # ------------------------------------------------------------------
    executar_etl = PythonOperator(
        task_id="executar_etl",
        python_callable=_executar_etl,
    )

    # ------------------------------------------------------------------
    # Task 4: Verifica a carga
    # ------------------------------------------------------------------
    verificar = PythonOperator(
        task_id="verificar_carga",
        python_callable=_verificar_carga,
    )

    # ------------------------------------------------------------------
    # Task 5: Branch — decide o caminho de notificacao
    # ------------------------------------------------------------------
    branch = BranchPythonOperator(
        task_id="branch_resultado",
        python_callable=_branch_resultado,
    )

    # ------------------------------------------------------------------
    # Tasks de notificacao
    # ------------------------------------------------------------------
    notificar_sucesso = EmailOperator(
        task_id="notificar_sucesso",
        to=["data-team@atlas.com.br"],
        subject="[ATLAS] Pipeline executado com sucesso — {{ ds }}",
        html_content="""
        <h2>Pipeline ATLAS concluido com sucesso</h2>
        <p><b>Data de referencia:</b> {{ ds }}</p>
        <p><b>Iniciado em:</b> {{ execution_date }}</p>
        <p>Todos os dados foram processados e carregados no Data Warehouse.</p>
        <hr>
        <p>Acesse o dashboard: <a href="http://localhost:8501">Streamlit</a></p>
        <p>API analitica: <a href="http://localhost:8000/docs">FastAPI</a></p>
        """,
    )

    notificar_falha = EmailOperator(
        task_id="notificar_falha",
        to=["data-team@atlas.com.br"],
        subject="[ATLAS] FALHA no pipeline — {{ ds }}",
        html_content="""
        <h2 style="color:red">Pipeline ATLAS falhou</h2>
        <p><b>Data de referencia:</b> {{ ds }}</p>
        <p>Verifique os logs no Airflow: <a href="http://localhost:8080">Airflow UI</a></p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    fim = EmptyOperator(
        task_id="fim",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ------------------------------------------------------------------
    # Dependencias (ordem de execucao)
    # ------------------------------------------------------------------
    # Todos os sensores -> validar -> executar_etl -> verificar -> branch
    for sensor in sensores:
        sensor >> validar

    validar >> executar_etl >> verificar >> branch
    branch >> [notificar_sucesso, notificar_falha]
    [notificar_sucesso, notificar_falha] >> fim
