"""
ATLAS ERP Pipeline — Configuration Settings
"""
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Data paths
# ---------------------------------------------------------------------------
RAW_DATA_DIR      = BASE_DIR / "data" / "raw"
PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"
REJECTED_DATA_DIR  = BASE_DIR / "data" / "rejected"

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DATABASE_DIR  = BASE_DIR / "database"
DATABASE_PATH = DATABASE_DIR / "erp_warehouse.db"

# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------
LOGS_DIR      = BASE_DIR / "logs"
LOG_FILE      = LOGS_DIR / "atlas_pipeline.log"
LOG_LEVEL     = "INFO"

# ---------------------------------------------------------------------------
# ETL behaviour
# ---------------------------------------------------------------------------
ENCODING         = "utf-8"
CSV_SEPARATOR    = ","
DATE_FORMAT      = "%Y-%m-%d"
DATE_FORMATS_ALT = ["%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"]  # fallback parsers
BATCH_SIZE       = 1_000

# ---------------------------------------------------------------------------
# Data-quality thresholds
# ---------------------------------------------------------------------------
MAX_NULL_PCT      = 0.10   # alert if any column has > 10 % nulls
MAX_DUPLICATE_PCT = 0.05   # alert if dataset has > 5 % duplicates

# ---------------------------------------------------------------------------
# Source file registry
# ---------------------------------------------------------------------------
SOURCE_FILES = {
    "produtos":              RAW_DATA_DIR / "produtos.csv",
    "clientes":              RAW_DATA_DIR / "clientes.csv",
    "fornecedores":          RAW_DATA_DIR / "fornecedores.csv",
    "pedidos_venda":         RAW_DATA_DIR / "pedidos_venda.csv",
    "itens_pedido_venda":    RAW_DATA_DIR / "itens_pedido_venda.csv",
    "pedidos_compra":        RAW_DATA_DIR / "pedidos_compra.csv",
    "itens_pedido_compra":   RAW_DATA_DIR / "itens_pedido_compra.csv",
    "movimentacoes_estoque": RAW_DATA_DIR / "movimentacoes_estoque.csv",
}

# ---------------------------------------------------------------------------
# Valid domain values
# ---------------------------------------------------------------------------
VALID_UNIDADES       = {"UN", "CX", "RS", "LI", "KG", "MT", "PC"}
VALID_STATUS_VENDA   = {"PROCESSANDO", "EM_SEPARACAO", "ENTREGUE", "CANCELADO",
                        "AGUARDANDO_APROVACAO", "DEVOLVIDO"}
VALID_STATUS_COMPRA  = {"AGUARDANDO", "RECEBIDO", "CANCELADO", "PARCIAL"}
VALID_TIPO_MOV       = {"ENTRADA", "SAIDA", "TRANSFERENCIA", "AJUSTE", "DEVOLUCAO"}
