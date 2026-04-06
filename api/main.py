"""
ATLAS ERP — API REST Analítica
================================
Fase 4 do Roadmap de Evolução para Produção.

Endpoints disponíveis:
  GET /estoque/                 — Posicao atual do estoque
  GET /estoque/criticos         — Produtos em estado critico
  GET /estoque/resumo           — Resumo por categoria
  GET /estoque/{cod_produto}    — Detalhe de um produto

  GET /vendas/                  — Lista pedidos de venda
  GET /vendas/kpis              — KPIs consolidados
  GET /vendas/por-periodo       — Receita mensal com MoM
  GET /vendas/margem            — Margem bruta por produto

  GET /qualidade/execucoes      — Historico de execucoes do pipeline
  GET /qualidade/rejeicoes      — Registros rejeitados
  GET /qualidade/anomalias      — Anomalias por Z-score

  GET /previsao/demanda         — Previsao de demanda (tendencia linear)
  GET /previsao/reposicao       — Sugestao de compra por cobertura

Documentacao interativa (Swagger): http://localhost:8000/docs
Documentacao alternativa (ReDoc):  http://localhost:8000/redoc

Como rodar:
  pip install fastapi uvicorn
  py -m uvicorn api.main:app --reload
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import estoque, vendas, qualidade, previsao

app = FastAPI(
    title="ATLAS ERP — API Analítica",
    description=(
        "API REST para consultas analíticas sobre o Data Warehouse do ATLAS ERP Pipeline. "
        "Fase 4 do Roadmap de Evolução para Produção."
    ),
    version="1.0.0",
    contact={
        "name": "ATLAS Pipeline",
        "url": "https://github.com/Daniel-1984/Engenharia-de-Dados",
    },
    license_info={"name": "MIT"},
)

# CORS — permite chamadas do dashboard Streamlit e outras origens locais
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(estoque.router)
app.include_router(vendas.router)
app.include_router(qualidade.router)
app.include_router(previsao.router)


@app.get("/", tags=["Root"], summary="Status da API")
def root():
    return {
        "api": "ATLAS ERP Analytics API",
        "versao": "1.0.0",
        "status": "online",
        "docs": "/docs",
        "redoc": "/redoc",
        "endpoints": {
            "estoque": "/estoque",
            "vendas": "/vendas",
            "qualidade": "/qualidade",
            "previsao": "/previsao",
        },
    }


@app.get("/health", tags=["Root"], summary="Health check")
def health():
    from pathlib import Path
    db = Path(__file__).resolve().parent.parent / "database" / "erp_warehouse.db"
    return {
        "status": "healthy" if db.exists() else "degraded",
        "database": "conectado" if db.exists() else "banco nao encontrado — execute py main.py",
        "tamanho_db_kb": round(db.stat().st_size / 1024, 1) if db.exists() else None,
    }
