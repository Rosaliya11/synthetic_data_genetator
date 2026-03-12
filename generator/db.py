"""
Database helpers: save generated data to SQLite (or insert into your own DB).
"""

import sqlite3
import os
from typing import List, Dict, Any, Optional


# Default table name for synthetic fraud data
DEFAULT_TABLE = "transactions"


def _sql_type(val: Any) -> str:
    if val is None:
        return "TEXT"
    if isinstance(val, bool):
        return "INTEGER"
    if isinstance(val, int):
        return "INTEGER"
    if isinstance(val, float):
        return "REAL"
    return "TEXT"


def save_to_sqlite(
    data: List[Dict[str, Any]],
    db_path: str,
    table: str = DEFAULT_TABLE,
    if_exists: str = "append",
) -> int:
    """
    Save a list of row dicts into a SQLite database.

    :param data: List of dicts (e.g. from SyntheticFraudGenerator.generate())
    :param db_path: Path to the .db file (e.g. "data/fraud_data.db")
    :param table: Table name (default "transactions")
    :param if_exists: "replace" = drop table and create; "append" = insert into existing or create
    :return: Number of rows inserted
    """
    if not data:
        return 0

    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        keys = list(data[0].keys())
        cols = ", ".join(f'"{k}"' for k in keys)
        placeholders = ", ".join("?" for _ in keys)

        if if_exists == "replace":
            conn.execute(f"DROP TABLE IF EXISTS \"{table}\"")
        cur = conn.execute(
            f'SELECT name FROM sqlite_master WHERE type="table" AND name=?',
            (table,),
        )
        if cur.fetchone() is None:
            # Create table from first row types
            def col_def(k: str) -> str:
                t = _sql_type(data[0].get(k))
                return f'"{k}" {t}'
            conn.execute(
                f'CREATE TABLE "{table}" ({", ".join(col_def(k) for k in keys)})'
            )

        for row in data:
            conn.execute(
                f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})',
                [row.get(k) for k in keys],
            )
        conn.commit()
        return len(data)
    finally:
        conn.close()


def load_from_sqlite(
    db_path: str,
    table: str = DEFAULT_TABLE,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Load rows from a SQLite table into a list of dicts.

    :param db_path: Path to the .db file
    :param table: Table name
    :param limit: Max rows to return (None = all)
    :return: List of row dicts
    """
    if not os.path.exists(db_path):
        return []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(f'SELECT * FROM "{table}"' + (f" LIMIT {limit}" if limit else ""))
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
