"""
ATLAS — Automated Transaction Loading & Analytics System
ERP Data Pipeline | Python + SQLite + Pandas

Usage:
    python main.py                   # pipeline completo + analytics
    python main.py --skip-analytics  # so o pipeline
    python main.py --analytics-only  # so os relatorios (banco ja gerado)
    python main.py --help
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime

from config.logging_config import setup_logging, get_logger

setup_logging("atlas")
logger = get_logger("main")


# =============================================================================
# Visual helpers  (rich)
# =============================================================================

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.text import Text
from rich import box
from rich.rule import Rule
from rich.align import Align
from rich.columns import Columns
from rich.padding import Padding

console = Console()


def print_banner() -> None:
    title = Text()
    title.append("  ATLAS\n", style="bold cyan")
    title.append("  Automated Transaction Loading & Analytics System\n", style="bold white")
    title.append("  ERP Data Pipeline  |  Python + SQLite + Pandas", style="dim white")

    console.print(Panel(title, border_style="cyan", padding=(1, 4)))


def print_step(step: int, total: int, entity: str, icon: str = ">>") -> None:
    console.print(
        f"  [dim][{step}/{total}][/dim]  [bold cyan]{icon}[/bold cyan]  "
        f"[bold white]{entity}[/bold white]"
    )


def print_entity_result(entity: str, input_r: int, valid_r: int, rejected_r: int, elapsed: float) -> None:
    rate = rejected_r / input_r * 100 if input_r else 0
    rejected_style = "red bold" if rejected_r > 0 else "green"

    console.print(
        f"     [dim]lido:[/dim] [white]{input_r:>4}[/white]  "
        f"[dim]valido:[/dim] [green]{valid_r:>4}[/green]  "
        f"[dim]rejeitado:[/dim] [{rejected_style}]{rejected_r:>3}[/{rejected_style}]  "
        f"[dim]({rate:.1f}%)[/dim]  "
        f"[dim]{elapsed:.2f}s[/dim]"
    )


def print_dq_report(reports: list[dict]) -> None:
    console.print()
    console.rule("[bold yellow]RELATORIO DE QUALIDADE DE DADOS", style="yellow")
    console.print()

    table = Table(
        box=box.ROUNDED,
        border_style="yellow",
        header_style="bold yellow",
        show_lines=True,
    )
    table.add_column("Entidade",         style="bold white",  width=26)
    table.add_column("Linhas",           style="cyan",        justify="right", width=7)
    table.add_column("Checks",           style="white",       justify="right", width=7)
    table.add_column("PASS",             style="green bold",  justify="right", width=6)
    table.add_column("WARN",             style="yellow bold", justify="right", width=6)
    table.add_column("FAIL",             style="red bold",    justify="right", width=6)
    table.add_column("Status",           style="white",       width=12)

    total_p = total_w = total_f = 0

    for r in reports:
        overall = r["overall"]
        if overall == "PASS":
            status_txt = Text("[OK]", style="bold green")
        elif overall == "WARN":
            status_txt = Text("[ATENCAO]", style="bold yellow")
        else:
            status_txt = Text("[FALHA]", style="bold red")

        table.add_row(
            r["entity"],
            f"{r['rows']:,}",
            str(len(r["checks"])),
            str(r["passed"]),
            str(r["warned"]),
            str(r["failed"]),
            status_txt,
        )
        total_p += r["passed"]
        total_w += r["warned"]
        total_f += r["failed"]

    table.add_section()
    table.add_row(
        "[bold]TOTAL[/bold]", "", str(total_p + total_w + total_f),
        str(total_p), str(total_w), str(total_f), "",
    )

    console.print(Align.center(table))

    if total_f > 0:
        console.print(f"\n  [red bold]>> {total_f} check(s) com FALHA[/red bold] — veja a pasta [yellow]data/rejected/[/yellow]")
    elif total_w > 0:
        console.print(f"\n  [yellow]>> {total_w} aviso(s)[/yellow] — verifique os logs para detalhes")
    else:
        console.print("\n  [bold green]Todos os checks de qualidade passaram![/bold green]")
    console.print()


def print_pipeline_summary(metrics) -> None:
    console.print()
    console.rule("[bold cyan]RESUMO DA EXECUCAO", style="cyan")
    console.print()

    # Summary cards
    cards = []

    t_input = Table.grid(padding=(0, 1))
    t_input.add_row("[dim]Total de linhas lidas[/dim]")
    t_input.add_row(f"[bold cyan]{metrics.total_input_rows:,}[/bold cyan]")
    cards.append(Panel(t_input, title="[dim]ENTRADA[/dim]", border_style="blue", width=20))

    t_valid = Table.grid(padding=(0, 1))
    t_valid.add_row("[dim]Linhas carregadas[/dim]")
    t_valid.add_row(f"[bold green]{metrics.total_valid_rows:,}[/bold green]")
    cards.append(Panel(t_valid, title="[dim]VALIDAS[/dim]", border_style="green", width=20))

    t_rej = Table.grid(padding=(0, 1))
    t_rej.add_row("[dim]Linhas rejeitadas[/dim]")
    color = "red" if metrics.total_rejected_rows > 0 else "green"
    t_rej.add_row(f"[bold {color}]{metrics.total_rejected_rows:,}[/bold {color}]")
    cards.append(Panel(t_rej, title="[dim]REJEITADAS[/dim]", border_style=color, width=20))

    t_time = Table.grid(padding=(0, 1))
    t_time.add_row("[dim]Tempo total[/dim]")
    t_time.add_row(f"[bold white]{metrics.elapsed_seconds:.2f}s[/bold white]")
    cards.append(Panel(t_time, title="[dim]TEMPO[/dim]", border_style="magenta", width=18))

    console.print(Align.center(Columns(cards)))

    # Per entity table
    console.print()
    etable = Table(
        box=box.SIMPLE_HEAVY,
        border_style="dim",
        header_style="bold dim",
        show_lines=False,
    )
    etable.add_column("Entidade",   style="white",       width=28)
    etable.add_column("Entrada",    style="cyan",        justify="right")
    etable.add_column("Validas",    style="green",       justify="right")
    etable.add_column("Rejeitadas", style="red",         justify="right")
    etable.add_column("Taxa Rejeicao", style="yellow",   justify="right")

    for e in metrics.entities:
        rate = e["rejected"] / e["input"] * 100 if e["input"] else 0
        rate_style = "red bold" if rate > 5 else ("yellow" if rate > 0 else "green")
        etable.add_row(
            e["entity"],
            str(e["input"]),
            str(e["valid"]),
            str(e["rejected"]),
            f"[{rate_style}]{rate:.1f}%[/{rate_style}]",
        )

    console.print(Align.center(etable))

    status_color = "green" if metrics.status == "SUCCESS" else ("yellow" if metrics.status == "PARTIAL" else "red")
    console.print()
    console.print(Align.center(
        Panel(
            f"[bold {status_color}]Pipeline: {metrics.status}[/bold {status_color}]\n"
            f"[dim]run_id: {metrics.pipeline_run_id}[/dim]\n"
            f"[dim]Log: logs/atlas_pipeline.log[/dim]",
            border_style=status_color,
            width=60,
        )
    ))
    console.print()


def print_analytics_header() -> None:
    console.print()
    console.rule("[bold magenta]RELATORIOS ANALITICOS", style="magenta")
    console.print()


# =============================================================================
# Pipeline runner (visual version)
# =============================================================================

def run_pipeline_visual() -> "PipelineMetrics":
    from etl.extract.csv_reader import CSVReader, CSVReadError
    from etl.load.sqlite_loader import SQLiteLoader
    from etl.transform.clientes_transformer import ClientesTransformer
    from etl.transform.estoque_transformer import EstoqueTransformer
    from etl.transform.pedidos_transformer import (
        ItensPedidoCompraTransformer, ItensPedidoVendaTransformer,
        PedidosCompraTransformer, PedidosVendaTransformer,
    )
    from etl.transform.produtos_transformer import ProdutosTransformer
    from etl.pipeline import ERPPipeline, PipelineMetrics
    from quality.validators import DataQualityValidator
    from quality.quality_report import QualityReport
    from config.settings import SOURCE_FILES
    import pandas as pd
    import uuid

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    metrics = PipelineMetrics(pipeline_run_id=run_id, started_at=datetime.now().isoformat())

    STEPS = [
        ("produtos",              ProdutosTransformer,            "stg_produtos",            "dim_produtos",            ["cod_produto", "descricao", "unidade", "custo_unitario", "preco_venda"]),
        ("clientes",              ClientesTransformer,            "stg_clientes",            "dim_clientes",            ["cod_cliente", "razao_social"]),
        ("fornecedores",          None,                           "stg_fornecedores",        "dim_fornecedores",        ["cod_fornecedor", "razao_social"]),
        ("pedidos_venda",         PedidosVendaTransformer,        "stg_pedidos_venda",       "fct_pedidos_venda",       ["num_pedido", "cod_cliente", "data_pedido", "status"]),
        ("itens_pedido_venda",    ItensPedidoVendaTransformer,    "stg_itens_pedido_venda",  "fct_itens_pedido_venda",  ["num_pedido", "seq_item", "cod_produto", "quantidade", "preco_unitario"]),
        ("pedidos_compra",        PedidosCompraTransformer,       "stg_pedidos_compra",      "fct_pedidos_compra",      ["num_oc", "cod_fornecedor", "data_oc", "status"]),
        ("itens_pedido_compra",   ItensPedidoCompraTransformer,   "stg_itens_pedido_compra", "fct_itens_pedido_compra", ["num_oc", "seq_item", "cod_produto", "quantidade", "custo_unitario"]),
        ("movimentacoes_estoque", EstoqueTransformer,             "stg_movimentacoes_estoque","fct_movimentacoes_estoque",["num_mov", "data_mov", "tipo_mov", "cod_produto", "quantidade"]),
    ]

    dq_reports = []
    total_steps = len(STEPS) + 1  # +1 for dim_tempo

    console.print()
    console.rule("[bold cyan]PIPELINE ETL", style="cyan")
    console.print(f"  [dim]run_id: {run_id}[/dim]")
    console.print()

    with Progress(
        SpinnerColumn(spinner_name="dots", style="cyan"),
        TextColumn("[bold white]{task.description}"),
        BarColumn(bar_width=30, style="cyan", complete_style="green"),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:

        main_task = progress.add_task("Processando entidades...", total=total_steps)

        with SQLiteLoader() as loader:
            loader.initialize_schema()

            for idx, (entity, transformer_cls, stg_table, ops_table, req_cols) in enumerate(STEPS, 1):
                progress.update(main_task, description=f"[cyan]{entity}[/cyan]")
                t0 = time.perf_counter()

                try:
                    reader = CSVReader(SOURCE_FILES[entity], required_columns=req_cols, pipeline_run_id=run_id)
                    raw_df = reader.read()
                except CSVReadError as exc:
                    logger.error(f"[{entity}] Extract failed: {exc}")
                    metrics.errors.append(str(exc))
                    loader.log_execution(run_id, entity, "EXTRACT_ERROR", 0, 0, 0, 0, str(exc))
                    progress.advance(main_task)
                    continue

                dq = DataQualityValidator(entity)
                dq_result = dq.run(raw_df, req_cols)
                dq_reports.append(dq_result)

                if transformer_cls is not None:
                    result        = transformer_cls().transform(raw_df)
                    valid_df      = result.valid
                    rejected_df   = result.rejected
                    input_rows    = result.metrics["input_rows"]
                    valid_rows    = result.metrics["valid_rows"]
                    rejected_rows = result.metrics["rejected_rows"]
                else:
                    valid_df      = raw_df.copy()
                    rejected_df   = pd.DataFrame()
                    input_rows    = len(raw_df)
                    valid_rows    = len(raw_df)
                    rejected_rows = 0

                loader.load(raw_df,    stg_table, mode="replace")
                loader.load(valid_df,  ops_table, mode="upsert")
                if not rejected_df.empty:
                    loader.load_rejected(rejected_df, entity)

                elapsed = time.perf_counter() - t0
                loader.log_execution(run_id, entity, "SUCCESS", input_rows, valid_rows, rejected_rows, elapsed)

                metrics.entities.append({"entity": entity, "input": input_rows, "valid": valid_rows, "rejected": rejected_rows})
                metrics.total_input_rows    += input_rows
                metrics.total_valid_rows    += valid_rows
                metrics.total_rejected_rows += rejected_rows

                progress.advance(main_task)

            # dim_tempo
            progress.update(main_task, description="[cyan]dim_tempo (tabela de datas)[/cyan]")
            from etl.pipeline import ERPPipeline
            ERPPipeline._build_dim_tempo(None, loader)
            progress.advance(main_task)

    console.print()
    print_dq_report(dq_reports)

    elapsed_total = (datetime.now() - datetime.fromisoformat(metrics.started_at)).total_seconds()
    metrics.finished_at     = datetime.now().isoformat()
    metrics.status          = "SUCCESS" if not metrics.errors else "PARTIAL"
    metrics.elapsed_seconds = round(elapsed_total, 2)

    print_pipeline_summary(metrics)
    return metrics


# =============================================================================
# Analytics runner (visual version)
# =============================================================================

def run_analytics_visual() -> None:
    from reports.analytics_runner import AnalyticsRunner, REPORT_CATALOG
    import sqlite3
    from config.settings import DATABASE_PATH, BASE_DIR

    print_analytics_header()

    QUERIES_DIR = BASE_DIR / "queries"

    LABELS = {
        "estoque_atual":          ("Posicao de Estoque Atual",                     "blue"),
        "pedidos_em_aberto":      ("Pedidos de Venda em Aberto",                   "yellow"),
        "produtos_abaixo_minimo": ("Alertas de Reposicao (Estoque Critico)",       "red"),
        "vendas_por_periodo":     ("Receita de Vendas por Periodo",                "green"),
        "top_clientes":           ("Top Clientes por Receita (Classificacao ABC)", "cyan"),
        "giro_estoque":           ("Giro de Estoque por Produto",                  "magenta"),
        "margem_por_produto":     ("Margem Bruta por Produto",                     "white"),
    }

    with sqlite3.connect(DATABASE_PATH) as conn:
        import pandas as pd

        for key, (sql_rel, title_color) in LABELS.items():
            sql_file, title = REPORT_CATALOG[key]
            sql = (QUERIES_DIR / sql_file).read_text(encoding="utf-8")

            try:
                df = pd.read_sql_query(sql, conn)
            except Exception as exc:
                console.print(f"  [red]Erro em {key}: {exc}[/red]")
                continue

            console.print(f"  [{title_color} bold]{title}[/{title_color} bold]  [dim]({len(df):,} linhas)[/dim]")

            if df.empty:
                console.print("  [dim](sem dados)[/dim]\n")
                continue

            # Build rich table
            rtable = Table(
                box=box.SIMPLE,
                border_style="dim",
                header_style=f"bold {title_color}",
                show_lines=False,
                row_styles=["", "dim"],   # zebra effect
            )

            for col in df.columns:
                justify = "right" if df[col].dtype in ["float64", "int64", "Int64"] else "left"
                rtable.add_column(col, justify=justify)

            # Show max 15 rows
            display_df = df.head(15)
            for _, row in display_df.iterrows():
                rtable.add_row(*[
                    f"{v:,.2f}" if isinstance(v, float) else str(v) if v is not None else ""
                    for v in row
                ])

            if len(df) > 15:
                rtable.add_row(*["..." for _ in df.columns])

            console.print(Padding(rtable, (0, 4)))
            console.print()


# =============================================================================
# Entry point
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="ATLAS ERP Data Pipeline")
    parser.add_argument("--skip-analytics", action="store_true", help="Executa so o pipeline ETL")
    parser.add_argument("--analytics-only", action="store_true", help="Executa so os relatorios")
    args = parser.parse_args()

    print_banner()

    t_start = time.perf_counter()
    logger.info(f"ATLAS started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.analytics_only:
        run_analytics_visual()
    else:
        metrics = run_pipeline_visual()

        if metrics.status in ("SUCCESS", "PARTIAL") and not args.skip_analytics:
            run_analytics_visual()
        elif metrics.status == "FAILURE":
            console.print("[red bold]Pipeline encerrou com erros criticos.[/red bold]")
            sys.exit(1)

    elapsed = time.perf_counter() - t_start
    logger.info(f"Total wall-clock time: {elapsed:.2f}s")
    console.print(f"  [dim]Tempo total: {elapsed:.2f}s  |  Log: logs/atlas_pipeline.log[/dim]\n")


if __name__ == "__main__":
    main()
