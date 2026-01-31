from __future__ import annotations

import os
from sqlalchemy import create_engine, text

from etl.common.logging import setup_logger

logger = setup_logger("harvest_etl")

def refresh_mv(concurrently: bool = False) -> None:
    """
    Refresh materialized view for harvest.
    - concurrently=True     : non-blocking (requires UNIQUE index)
    - concurrently=False    : blocking but simple (default)
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")

    engine = create_engine(url, future=True)

    if concurrently:
        sql = "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_harvest_monthly"
    else:
        sql = "REFRESH MATERIALIZED VIEW mv_harvest_monthly"

    # CONCURRENTLY　はトランザクション外で実行する必要あり
    if concurrently:
        with engine.connect() as conn:
            conn.execute(text(sql))
    else:
        with engine.begin() as conn:
            conn.execute(text(sql))

    logger.info(
            f"refresh_mv done concurrenlty={concurrently}"
    )
