"""
ATLAS API — Endpoints de Qualidade de Dados e Observabilidade
"""
import sqlite3
from pathlib import Path

from fastapi import APIRouter, Depends

from api.database import get_db

router = APIRouter(prefix="/qualidade", tags=["Qualidade de Dados"])

REJECTED_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "rejected"


@router.get("/execucoes", summary="Historico de execucoes do pipeline")
def get_execucoes(conn: sqlite3.Connection = Depends(get_db)):
    """Retorna o log de todas as execucoes do pipeline ETL com metricas."""
    rows = conn.execute("""
        SELECT
            run_id, entidade, status,
            linhas_lidas, linhas_validas, linhas_rejeitadas,
            inicio_execucao, fim_execucao,
            ROUND(
                (julianday(fim_execucao) - julianday(inicio_execucao)) * 86400,
            2) AS duracao_segundos,
            erro_mensagem
        FROM log_etl_execucoes
        ORDER BY inicio_execucao DESC
        LIMIT 100
    """).fetchall()
    return [dict(r) for r in rows]


@router.get("/rejeicoes", summary="Registros rejeitados pela qualidade de dados")
def get_rejeicoes(conn: sqlite3.Connection = Depends(get_db)):
    """
    Retorna resumo dos registros rejeitados por entidade e motivo.
    Os arquivos completos ficam em data/rejected/.
    """
    rejected_files = []
    if REJECTED_DIR.exists():
        for f in sorted(REJECTED_DIR.glob("*.csv")):
            rejected_files.append({
                "arquivo": f.name,
                "tamanho_bytes": f.stat().st_size,
                "modificado_em": f.stat().st_mtime,
            })

    rows = conn.execute("""
        SELECT
            entidade,
            SUM(linhas_rejeitadas)  AS total_rejeitadas,
            SUM(linhas_lidas)       AS total_lidas,
            ROUND(
                CAST(SUM(linhas_rejeitadas) AS FLOAT)
                / NULLIF(SUM(linhas_lidas), 0) * 100,
            2)                      AS taxa_rejeicao_pct
        FROM log_etl_execucoes
        WHERE linhas_rejeitadas > 0
        GROUP BY entidade
        ORDER BY total_rejeitadas DESC
    """).fetchall()

    return {
        "por_entidade": [dict(r) for r in rows],
        "arquivos_rejeitados": rejected_files,
    }


@router.get("/anomalias", summary="Deteccao de anomalias por Z-score")
def get_anomalias(conn: sqlite3.Connection = Depends(get_db)):
    """
    Detecta pedidos com valor anomalo usando Z-score.
    Z > 2.5 = possivel anomalia (valor muito acima ou abaixo da media).
    """
    rows = conn.execute("""
        WITH stats AS (
            SELECT
                AVG(valor_total)  AS media,
                -- desvio padrao amostral (SQLite nao tem STDDEV, calculamos manualmente)
                SQRT(
                    SUM((valor_total - (SELECT AVG(valor_total) FROM fct_pedidos_venda)) *
                        (valor_total - (SELECT AVG(valor_total) FROM fct_pedidos_venda)))
                    / NULLIF(COUNT(*) - 1, 0)
                )                 AS desvio_padrao
            FROM fct_pedidos_venda
            WHERE status_pedido NOT IN ('CANCELADO', 'DEVOLVIDO')
        ),
        scored AS (
            SELECT
                v.num_pedido,
                v.cod_cliente,
                c.nome_cliente,
                v.valor_total,
                v.data_pedido,
                v.status_pedido,
                ROUND(ABS(v.valor_total - s.media) / NULLIF(s.desvio_padrao, 0), 2) AS z_score
            FROM fct_pedidos_venda v
            LEFT JOIN dim_clientes c ON v.cod_cliente = c.cod_cliente
            CROSS JOIN stats s
            WHERE v.status_pedido NOT IN ('CANCELADO', 'DEVOLVIDO')
        )
        SELECT * FROM scored
        WHERE z_score > 2.5
        ORDER BY z_score DESC
    """).fetchall()

    return {
        "total_anomalias": len(rows),
        "threshold_z_score": 2.5,
        "anomalias": [dict(r) for r in rows],
    }
