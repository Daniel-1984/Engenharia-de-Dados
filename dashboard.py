"""
ATLAS — Dashboard Streamlit
Visualizacao interativa do Data Warehouse ERP

Como rodar:
    streamlit run dashboard.py
"""
from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Config da pagina
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="ATLAS — ERP Data Pipeline",
    page_icon="🔷",
    layout="wide",
    initial_sidebar_state="expanded",
)

BASE_DIR = Path(__file__).resolve().parent
DB_PATH  = BASE_DIR / "database" / "erp_warehouse.db"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=30)
def query(sql: str) -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(sql, conn)
    # SQLite can return bytes for BLOB columns — convert to str for Plotly/JSON
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda v: v.decode("utf-8", errors="replace") if isinstance(v, bytes) else v)
    return df


def card(col, label: str, value, delta=None, color: str = "normal"):
    with col:
        st.metric(label=label, value=value, delta=delta, delta_color=color)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("## 🔷 ATLAS")
    st.markdown("**Automated Transaction Loading**  \n**& Analytics System**")
    st.markdown("---")

    db_exists = DB_PATH.exists()
    if db_exists:
        st.success("Banco conectado", icon="✅")
        size_kb = round(DB_PATH.stat().st_size / 1024, 1)
        st.caption(f"Tamanho: {size_kb} KB")
    else:
        st.error("Banco nao encontrado", icon="❌")
        st.caption("Execute o pipeline primeiro:")
        st.code("py main.py --skip-analytics")

    st.markdown("---")

    if st.button("🔄 Executar Pipeline", use_container_width=True, type="primary"):
        with st.spinner("Executando pipeline ETL..."):
            try:
                result = subprocess.run(
                    [sys.executable, "main.py", "--skip-analytics"],
                    capture_output=True, text=True, cwd=BASE_DIR, timeout=120
                )
                if result.returncode == 0:
                    st.success("Pipeline concluido!")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(f"Erro: {result.stderr[-500:]}")
            except Exception as e:
                st.error(f"Falha ao executar: {e}")

    st.markdown("---")
    st.caption("Python • Pandas • SQLite")
    st.caption("Portfolio de Data Engineering")


# ---------------------------------------------------------------------------
# Guard: banco nao existe
# ---------------------------------------------------------------------------
if not db_exists:
    st.title("🔷 ATLAS — ERP Data Pipeline")
    st.warning("Execute o pipeline primeiro para gerar o banco de dados.")
    st.code("py main.py --skip-analytics", language="bash")
    st.stop()


# ---------------------------------------------------------------------------
# Tabs principais
# ---------------------------------------------------------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Visao Geral",
    "📦 Estoque",
    "💰 Vendas",
    "🛒 Compras",
    "🔍 Qualidade de Dados",
])


# ============================================================================
# TAB 1 — VISAO GERAL
# ============================================================================
with tab1:
    st.title("🔷 ATLAS — ERP Data Pipeline")
    st.markdown("Dashboard executivo com os principais indicadores do ERP.")
    st.markdown("---")

    # KPIs
    receita_df  = query("SELECT ROUND(SUM(valor_liquido),2) AS total FROM vw_vendas_consolidadas")
    pedidos_df  = query("SELECT COUNT(*) AS total FROM fct_pedidos_venda WHERE status != 'CANCELADO'")
    produtos_df = query("SELECT COUNT(*) AS total FROM dim_produtos WHERE _is_active=1")
    estoque_df  = query("SELECT ROUND(SUM(valor_estoque),2) AS total FROM vw_estoque_atual")
    clientes_df = query("SELECT COUNT(DISTINCT cod_cliente) AS total FROM fct_pedidos_venda")
    oc_df       = query("SELECT COUNT(*) AS total FROM fct_pedidos_compra WHERE status='AGUARDANDO'")

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    card(c1, "Receita Total (R$)",     f"R$ {receita_df['total'].iloc[0]:,.0f}" if not receita_df.empty and receita_df['total'].iloc[0] else "R$ 0")
    card(c2, "Pedidos Ativos",         int(pedidos_df['total'].iloc[0]) if not pedidos_df.empty else 0)
    card(c3, "Produtos Cadastrados",   int(produtos_df['total'].iloc[0]) if not produtos_df.empty else 0)
    card(c4, "Valor em Estoque (R$)",  f"R$ {estoque_df['total'].iloc[0]:,.0f}" if not estoque_df.empty and estoque_df['total'].iloc[0] else "R$ 0")
    card(c5, "Clientes Ativos",        int(clientes_df['total'].iloc[0]) if not clientes_df.empty else 0)
    card(c6, "OCs Aguardando",         int(oc_df['total'].iloc[0]) if not oc_df.empty else 0)

    st.markdown("---")

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Receita Mensal")
        vendas_mes = query("""
            SELECT t.mes, t.nome_mes, t.ano,
                   ROUND(SUM(iv.valor_total_item),2) AS receita
            FROM fct_pedidos_venda pv
            JOIN fct_itens_pedido_venda iv ON pv.num_pedido = iv.num_pedido
            JOIN dim_tempo t ON t.data = pv.data_pedido
            WHERE pv.status != 'CANCELADO'
            GROUP BY t.ano, t.mes, t.nome_mes
            ORDER BY t.ano, t.mes
        """)
        if not vendas_mes.empty:
            fig = px.bar(
                vendas_mes, x="nome_mes", y="receita",
                color_discrete_sequence=["#00d4ff"],
                labels={"nome_mes": "Mes", "receita": "Receita (R$)"},
                text_auto=".2s",
            )
            fig.update_layout(
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                font_color="white", xaxis_title="", yaxis_title="R$",
            )
            st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Receita por Segmento de Cliente")
        seg_df = query("""
            SELECT c.segmento, ROUND(SUM(iv.valor_total_item),2) AS receita
            FROM fct_pedidos_venda pv
            JOIN dim_clientes c ON pv.cod_cliente = c.cod_cliente
            JOIN fct_itens_pedido_venda iv ON pv.num_pedido = iv.num_pedido
            WHERE pv.status != 'CANCELADO'
            GROUP BY c.segmento
            ORDER BY receita DESC
        """)
        if not seg_df.empty:
            fig2 = px.pie(
                seg_df, names="segmento", values="receita",
                color_discrete_sequence=px.colors.qualitative.Set3,
            )
            fig2.update_layout(
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                font_color="white",
            )
            st.plotly_chart(fig2, use_container_width=True)

    # Ultimas execucoes ETL
    st.subheader("Historico de Execucoes do Pipeline")
    etl_log = query("""
        SELECT pipeline_run_id, entity, status, input_rows, valid_rows, rejected_rows,
               ROUND(elapsed_seconds,2) AS elapsed_s, executed_at
        FROM log_etl_execucoes
        ORDER BY executed_at DESC
        LIMIT 20
    """)
    if not etl_log.empty:
        def color_status(v):
            if v == "SUCCESS": return "background-color: #1a3a1a; color: #00ff88"
            if v == "PARTIAL": return "background-color: #3a2a00; color: #ffaa00"
            return "background-color: #3a0000; color: #ff4444"
        st.dataframe(
            etl_log.style.map(color_status, subset=["status"]),
            use_container_width=True, height=280,
        )


# ============================================================================
# TAB 2 — ESTOQUE
# ============================================================================
with tab2:
    st.title("📦 Gestao de Estoque")
    st.markdown("---")

    estoque = query("SELECT * FROM vw_estoque_atual ORDER BY status_estoque, saldo_atual")

    if estoque.empty:
        st.info("Sem dados de estoque.")
    else:
        c1, c2, c3 = st.columns(3)
        criticos = len(estoque[estoque["status_estoque"] == "CRITICO"])
        alertas  = len(estoque[estoque["status_estoque"] == "ALERTA"])
        ok       = len(estoque[estoque["status_estoque"] == "OK"])

        with c1: st.metric("Critico (reposicao urgente)", criticos, delta=f"-{criticos}" if criticos else None, delta_color="inverse")
        with c2: st.metric("Alerta (estoque baixo)", alertas)
        with c3: st.metric("OK", ok, delta=str(ok))

        st.markdown("---")

        col_l, col_r = st.columns([3, 2])

        with col_l:
            st.subheader("Posicao de Estoque por Produto")
            fig = px.bar(
                estoque.head(20),
                x="descricao", y="saldo_atual",
                color="status_estoque",
                color_discrete_map={"CRITICO": "#ff4444", "ALERTA": "#ffaa00", "OK": "#00cc66"},
                labels={"descricao": "", "saldo_atual": "Saldo"},
                title="Top 20 produtos — saldo atual vs minimo",
            )
            fig.add_scatter(
                x=estoque.head(20)["descricao"],
                y=estoque.head(20)["estoque_minimo"],
                mode="markers", marker=dict(color="white", size=8, symbol="line-ew"),
                name="Estoque Minimo",
            )
            fig.update_layout(
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                font_color="white", xaxis_tickangle=-45,
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_r:
            st.subheader("Valor em Estoque por Categoria")
            cat_df = estoque.groupby("categoria")["valor_estoque"].sum().reset_index()
            fig2 = px.pie(
                cat_df, names="categoria", values="valor_estoque",
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
            fig2.update_layout(
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="white",
            )
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Tabela Completa de Estoque")

        def color_status_estoque(v):
            if v == "CRITICO": return "background-color: #3a0000; color: #ff4444; font-weight: bold"
            if v == "ALERTA":  return "background-color: #3a2a00; color: #ffaa00"
            return "color: #00cc66"

        estoque_display = estoque[["cod_produto","descricao","categoria","unidade",
                                    "saldo_atual","estoque_minimo","estoque_maximo",
                                    "valor_estoque","status_estoque"]].copy()
        estoque_display["valor_estoque"] = estoque_display["valor_estoque"].map("R$ {:,.2f}".format)

        st.dataframe(
            estoque_display.style.map(color_status_estoque, subset=["status_estoque"]),
            use_container_width=True, height=450,
        )


# ============================================================================
# TAB 3 — VENDAS
# ============================================================================
with tab3:
    st.title("💰 Analise de Vendas")
    st.markdown("---")

    # Top clientes
    top_cli = query("""
        WITH cr AS (
            SELECT cod_cliente, cliente, segmento, estado,
                   COUNT(DISTINCT num_pedido) AS pedidos,
                   ROUND(SUM(valor_liquido),2) AS receita,
                   ROUND(AVG(valor_liquido),2) AS ticket
            FROM vw_vendas_consolidadas GROUP BY cod_cliente, cliente, segmento, estado
        ), total AS (SELECT SUM(receita) AS t FROM cr)
        SELECT cr.*, ROUND(cr.receita/t.t*100,1) AS share_pct,
               SUM(cr.receita) OVER (ORDER BY cr.receita DESC
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acum,
               t.t AS total_geral,
               CASE WHEN SUM(cr.receita) OVER (ORDER BY cr.receita DESC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)/t.t <= 0.8 THEN 'A'
                    WHEN SUM(cr.receita) OVER (ORDER BY cr.receita DESC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)/t.t <= 0.95 THEN 'B'
                    ELSE 'C' END AS classe_abc
        FROM cr, total t ORDER BY receita DESC
    """)

    # Margem
    margem = query("""
        SELECT p.descricao, p.categoria,
               SUM(iv.quantidade) AS qtd,
               ROUND(SUM(iv.valor_total_item),2) AS receita,
               ROUND(SUM(iv.quantidade*p.custo_unitario),2) AS cmv,
               ROUND(SUM(iv.valor_total_item)-SUM(iv.quantidade*p.custo_unitario),2) AS margem,
               ROUND((SUM(iv.valor_total_item)-SUM(iv.quantidade*p.custo_unitario))
                     /NULLIF(SUM(iv.valor_total_item),0)*100, 1) AS margem_pct
        FROM fct_itens_pedido_venda iv
        JOIN dim_produtos p ON iv.cod_produto=p.cod_produto
        JOIN fct_pedidos_venda pv ON iv.num_pedido=pv.num_pedido
        WHERE pv.status NOT IN ('CANCELADO','DEVOLVIDO')
        GROUP BY p.cod_produto, p.descricao, p.categoria
        ORDER BY margem DESC
    """)

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Top Clientes por Receita")
        if not top_cli.empty:
            fig = px.bar(
                top_cli.head(10), x="cliente", y="receita",
                color="classe_abc",
                color_discrete_map={"A": "#00cc66", "B": "#ffaa00", "C": "#ff4444"},
                text="share_pct",
                labels={"cliente": "", "receita": "Receita (R$)"},
            )
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig.update_layout(
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                font_color="white", xaxis_tickangle=-30,
            )
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Margem Bruta por Categoria")
        if not margem.empty:
            cat_mg = margem.groupby("categoria").agg({"receita":"sum","margem":"sum"}).reset_index()
            cat_mg["margem_pct"] = (cat_mg["margem"] / cat_mg["receita"] * 100).round(1)
            fig2 = px.bar(
                cat_mg.sort_values("margem", ascending=True),
                x="margem", y="categoria", orientation="h",
                color="margem_pct",
                color_continuous_scale="RdYlGn",
                labels={"margem": "Margem (R$)", "categoria": "", "margem_pct": "Margem %"},
                text="margem_pct",
            )
            fig2.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig2.update_layout(
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="white",
            )
            st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Classificacao ABC dos Clientes")
    if not top_cli.empty:
        top_display = top_cli[["cliente","segmento","estado","pedidos","receita","ticket","share_pct","classe_abc"]].copy()
        top_display["receita"] = top_display["receita"].map("R$ {:,.2f}".format)
        top_display["ticket"]  = top_display["ticket"].map("R$ {:,.2f}".format)

        def cor_abc(v):
            if v == "A": return "background-color: #1a3a1a; color: #00ff88; font-weight: bold"
            if v == "B": return "background-color: #3a2a00; color: #ffaa00"
            return "background-color: #1a1a2e; color: #8888ff"

        st.dataframe(
            top_display.style.map(cor_abc, subset=["classe_abc"]),
            use_container_width=True, height=350,
        )

    st.markdown("---")
    st.subheader("Status dos Pedidos de Venda")
    status_df = query("""
        SELECT status, COUNT(*) AS qtd,
               ROUND(SUM(iv.valor_total_item),2) AS valor
        FROM fct_pedidos_venda pv
        JOIN fct_itens_pedido_venda iv ON pv.num_pedido=iv.num_pedido
        GROUP BY status ORDER BY qtd DESC
    """)
    if not status_df.empty:
        fig3 = px.pie(
            status_df, names="status", values="qtd",
            color="status",
            color_discrete_map={
                "ENTREGUE": "#00cc66", "PROCESSANDO": "#00aaff",
                "EM_SEPARACAO": "#ffaa00", "CANCELADO": "#ff4444",
                "AGUARDANDO_APROVACAO": "#aa44ff",
            },
        )
        fig3.update_layout(
            plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="white",
        )
        c1, c2 = st.columns([1, 2])
        with c1: st.plotly_chart(fig3, use_container_width=True)
        with c2:
            st.dataframe(status_df, use_container_width=True)


# ============================================================================
# TAB 4 — COMPRAS
# ============================================================================
with tab4:
    st.title("🛒 Gestao de Compras")
    st.markdown("---")

    compras = query("""
        SELECT pc.num_oc, pc.data_oc, pc.status, f.razao_social AS fornecedor,
               COUNT(ic.seq_item) AS itens,
               ROUND(SUM(ic.valor_total_item),2) AS valor_total
        FROM fct_pedidos_compra pc
        JOIN dim_fornecedores f ON pc.cod_fornecedor=f.cod_fornecedor
        JOIN fct_itens_pedido_compra ic ON pc.num_oc=ic.num_oc
        GROUP BY pc.num_oc, pc.data_oc, pc.status, f.razao_social
        ORDER BY pc.data_oc DESC
    """)

    forn_df = query("""
        SELECT f.razao_social AS fornecedor, f.prazo_entrega_dias,
               COUNT(DISTINCT pc.num_oc) AS total_ocs,
               ROUND(SUM(ic.valor_total_item),2) AS total_comprado
        FROM fct_pedidos_compra pc
        JOIN dim_fornecedores f ON pc.cod_fornecedor=f.cod_fornecedor
        JOIN fct_itens_pedido_compra ic ON pc.num_oc=ic.num_oc
        WHERE pc.status != 'CANCELADO'
        GROUP BY f.cod_fornecedor, f.razao_social, f.prazo_entrega_dias
        ORDER BY total_comprado DESC
    """)

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Compras por Fornecedor")
        if not forn_df.empty:
            fig = px.bar(
                forn_df, x="fornecedor", y="total_comprado",
                color_discrete_sequence=["#7c4dff"],
                text_auto=".2s",
                labels={"fornecedor": "", "total_comprado": "R$ Comprado"},
            )
            fig.update_layout(
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                font_color="white", xaxis_tickangle=-30,
            )
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Status das Ordens de Compra")
        if not compras.empty:
            status_oc = compras.groupby("status")["valor_total"].sum().reset_index()
            fig2 = px.pie(
                status_oc, names="status", values="valor_total",
                color="status",
                color_discrete_map={
                    "RECEBIDO": "#00cc66", "AGUARDANDO": "#ffaa00", "CANCELADO": "#ff4444",
                },
            )
            fig2.update_layout(
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="white",
            )
            st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Todas as Ordens de Compra")
    if not compras.empty:
        compras["valor_total"] = compras["valor_total"].map("R$ {:,.2f}".format)

        def cor_oc(v):
            if v == "RECEBIDO": return "color: #00cc66"
            if v == "AGUARDANDO": return "color: #ffaa00"
            if v == "CANCELADO": return "color: #ff4444"
            return ""

        st.dataframe(
            compras.style.map(cor_oc, subset=["status"]),
            use_container_width=True, height=400,
        )


# ============================================================================
# TAB 5 — QUALIDADE DE DADOS
# ============================================================================
with tab5:
    st.title("🔍 Qualidade de Dados & Observabilidade")
    st.markdown("---")

    rejected = query("""
        SELECT entity, reject_reason, logged_at,
               SUBSTR(row_data, 1, 120) AS row_preview
        FROM log_registros_rejeitados
        ORDER BY logged_at DESC
    """)

    etl_hist = query("""
        SELECT pipeline_run_id, entity, status,
               input_rows, valid_rows, rejected_rows,
               ROUND(elapsed_seconds,2) AS elapsed_s,
               executed_at
        FROM log_etl_execucoes
        ORDER BY executed_at DESC
    """)

    c1, c2, c3 = st.columns(3)
    total_rej = int(rejected["entity"].count()) if not rejected.empty else 0
    entidades = int(etl_hist["entity"].nunique()) if not etl_hist.empty else 0
    runs = int(etl_hist["pipeline_run_id"].nunique()) if not etl_hist.empty else 0
    c1.metric("Total de Registros Rejeitados", total_rej)
    c2.metric("Entidades Monitoradas", entidades)
    c3.metric("Execucoes do Pipeline", runs)

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Rejeicoes por Entidade")
        if not rejected.empty:
            rej_chart = rejected.groupby(["entity","reject_reason"]).size().reset_index(name="qtd")
            fig = px.bar(
                rej_chart, x="entity", y="qtd", color="reject_reason",
                labels={"entity": "Entidade", "qtd": "Rejeicoes", "reject_reason": "Motivo"},
                color_discrete_sequence=px.colors.qualitative.Set1,
            )
            fig.update_layout(
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="white",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.success("Nenhuma rejeicao registrada!")

    with col_r:
        st.subheader("Taxa de Rejeicao por Entidade (ultima execucao)")
        if not etl_hist.empty:
            last_run = etl_hist.iloc[0]["pipeline_run_id"]
            last_run_df = etl_hist[etl_hist["pipeline_run_id"] == last_run].copy()
            last_run_df["taxa_rejeicao"] = (
                last_run_df["rejected_rows"] / last_run_df["input_rows"].replace(0, 1) * 100
            ).round(1)
            fig2 = px.bar(
                last_run_df, x="entity", y="taxa_rejeicao",
                color="taxa_rejeicao",
                color_continuous_scale="RdYlGn_r",
                labels={"entity": "", "taxa_rejeicao": "Taxa (%)"},
                text="taxa_rejeicao",
            )
            fig2.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig2.update_layout(
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="white",
            )
            st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Registros Rejeitados — Detalhes")
    if not rejected.empty:
        def cor_reason(v):
            if "DUPLICATE" in str(v): return "color: #ffaa00"
            if "BUSINESS" in str(v):  return "color: #ff4444"
            return "color: #8888ff"
        st.dataframe(
            rejected.style.map(cor_reason, subset=["reject_reason"]),
            use_container_width=True, height=300,
        )
    else:
        st.success("Sem registros rejeitados nesta execucao!")

    st.markdown("---")
    st.subheader("Log de Execucoes do Pipeline")
    if not etl_hist.empty:
        def cor_status_log(v):
            if v == "SUCCESS": return "background-color: #1a3a1a; color: #00ff88"
            if v == "EXTRACT_ERROR": return "background-color: #3a0000; color: #ff4444"
            return ""
        st.dataframe(
            etl_hist.style.map(cor_status_log, subset=["status"]),
            use_container_width=True, height=400,
        )
