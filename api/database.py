"""
ATLAS API — Database connection helper
"""
import sqlite3
from pathlib import Path
from typing import Generator

from fastapi import HTTPException

DB_PATH = Path(__file__).resolve().parent.parent / "database" / "erp_warehouse.db"


def get_db() -> Generator:
    """Dependency que entrega uma conexão SQLite e fecha ao final."""
    if not DB_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail="Banco de dados nao encontrado. Execute 'py main.py' primeiro.",
        )
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # retorna dicts ao invés de tuplas
    try:
        yield conn
    finally:
        conn.close()
