from __future__ import annotations

import os
from typing import Any, Dict, List

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert

from etl.common.logging import setup_logger

logger = setup_logger("harvest_etl")

def _get_engine() -> Engine:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set. Example: postgresql+psycopg://user:pass@host:5432/dbname")
    # future=True　は　SQLAlchemy 1.4 +　の2.0互換
    return create_engine(url, future=True, pool_pre_ping=True)

def _get_harvest_table(engine: Engine) -> Table:
    """
    既存DBの　harvest テーブルをリフレクションで取得する。
    Alembic管理でも、ETL単体でも動く。
    """
    md = MetaData()
    return Table("harvest", md, autoload_with=engine)

def load_rows(rows: List[Dict[str, Any]]) -> None:
    """
    rows 例:
    {"company": "...", "crop":"...", "month":"2026-01", "total_kg":12.3}
    """
    if not rows:
        logger.info("load skipped: rows is empty")
        return

    engine = _get_engine()
    harvest = _get_harvest_table(engine)

    # UPSERTの自然キー（仮）
    conflict_cols = ["company", "crop", "month"]

    stmt = insert(harvest).values(rows)

    # 更新対象：キー以外
    update_cols = {
        c.name: stmt.excluded[c.name]
        for c in harvest.columns
        if c.name not in conflict_cols
    }

    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=conflict_cols,
        set_=update_cols,
    )

    # CSV単位 transaction　は　run.py 側で包む設計もあるが、
    # まずは　Load_rows を　“1回=1トランザクション”として安定させる
    with engine.begin() as conn:
        conn.execute(upsert_stmt)

    logger.info(f"upserted rows={len(rows)}")
