"""
ATLAS API — Database connection helper
"""
import os
import sqlite3
from pathlib import Path
from typing import Generator

from fastapi import HTTPException

# Suporta variavel de ambiente para override no Render/nuvem
_default = Path(__file__).resolve().parent.parent / "database" / "erp_warehouse.db"
DB_PATH = Path(os.environ.get("ATLAS_DB_PATH", str(_default)))


def get_db() -> Generator:
    """Dependency que entrega uma conexão SQLite e fecha ao final."""
    if not DB_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail="Banco de dados nao encontrado. Execute 'python main.py' primeiro.",
        )
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # retorna dicts ao invés de tuplas
    try:
        yield conn
    finally:
        conn.close()
