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
        raise RuntimeError("DATABASE_URL is not set")
    return create_engine(url, future=True, pool_pre_ping=True)

def _table(engine: Engine, name: str) -> Table:
    md = MetaData()
    return Table(name, md, autoload_with=engine)

# legacy monthly: harvest
def load_rows(rows: List[Dict[str, Any]]) -> None:
    """
    rows 例:
    {"company": "...", "crop":"...", "month":"2026-01", "total_kg":12.3}
    """
    if not rows:
        logger.info("load skipped: rows is empty")
        return

    engine = _get_engine()
    harvest = _table(engine, "harvest")

    stmt = insert(harvest).values(rows)

    conflict_cols = ["company", "crop", "month"]
    update_cols = {
        c.name: stmt.excluded[c.name]
        for c in harvest.columns
            if c.name not in conflict_cols
    }

    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=conflict_cols,
        set_=update_cols,
    )

    with engine.begin() as conn:
        conn.execute(upsert_stmt)

    logger.info(f"upserted rows={len(rows)}")

# fact: harvest_fact
def load_fact_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        logger.info("fact load skipped: rows is empty")
        return

    engine = _get_engine()
    fact = _table(engine, "harvest_fact")

    stmt = insert(fact).values(rows)

    #まずは「重複はDBに任せる」最小構成（止まったら次でduplicate detectorへ）
    with engine.begin() as conn:
        conn.execute(stmt)

    logger.info(f"inserted fact rows={len(rows)}")

# quarantine: harvest_quarantine
def load_quarantine_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        logger.info("quarantine(dict) load skipped: rows is empty")
        return
        engine = _get_gengine()
        hq = _table(engine, "harvest_quarantine")

        allowed = {c.name for c in hq.columns}
        for i, r in enumerate(rows):
            extra = set(r.keys()) - allowed
            if extra:
                raise RuntimeError(f"quarantine(dict) has extra keys idex={i} extra={sorted(extra)}")

        stmt = insert(hq).values(rows)
        with engine.begin() as conn:
            conn.execute(stmt)

        logger.info(f"inserted quarantine(dict) rows={len(rows)}")
