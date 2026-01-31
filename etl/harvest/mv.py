from __future__ import annotations

import os
from sqlalchemy import create_engine, text
from etl.common.logging import setup_logger

logger = setup_logger("harvest_etl")

MV_NAME = "mv_harvest_monthly"

def refresh_mv(concurrently: bool = False) -> None:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")

    engine = create_engine(url, future=True, pool_pre_ping=True)

    if concurrently:
        sql = f"REFRESH MATERIALIZED VIEW CONCURRENTLY {MV_NAME}"
        # CONCURRENLTY はトランザクション外
        try:
            with engine.connect() as conn:
                conn.execute(text(sql))
            logger.info("refresh_mv done concurrently=True")
        except Exception as e:
            logger.warning("refresh_mv failed concurrently=True reason=%s", e, exc_info=True)
    else:
        sql = f"REFRESH MATERIALIZED VIEW {MV_NAME}"
        try:
            with engine.begin() as conn:
                conn.execute(text(sql))
            logger.info("refresh_mv done concurrently=False")
        except Exception as e:
            logger.warning("refresh_mv failed concurrently=False reason=$s", e, exc_info=True)
