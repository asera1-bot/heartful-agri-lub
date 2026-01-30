from __future__ import annotations

from pathlib import Path
from typing import Iterable

from etl.common.logging import setup_logger
from etl.common.retry import with_retry

from etl.harvest.extract import extract_csv
from etl.harvest.colmap import rename_columns
from etl.harvest.validate import validate_rows
from etl.harvest.transform import transform_rows
from etl.harvest.load import load_rows
from etl.harvest.mv import refresh_mv

logger = setup_logger("harvest_etl")

def run(csv_files: Iterable[Path]) -> None:
    ok = 0
    ng = 0

    for csv_path in csv_files:
        logger.info(f"start csv={csv_path.name}")

        try:
            def process() -> None:
                # 1) exrract
                df = extract_csv(csv_path)

                # extractが空DFを返した場合はCSVスキップ扱い
                if df.empty:
                    raise ValueError("empty dataframe (empty file or no rows)")

                # 2) colmap　（列名だけ正規化）
                df = df.rename(columns=rename_columns(list(df.columns)))
                
                # 3) validate　（行単位スキップ込み）
                rows = validate_rows(df)
                if not rows:
                    raise ValueError("no valid rows after validation")

                # 4) transform
                rows = transform_rows(rows)

                # 5) Load
                load_rows(rows)

            with_retry(process)
            ok += 1
            logger.info(f"success csv={csv_path.name}")

        except Exception as e:
            ng += 1
            logger.error(f"failed csv={csv_path.name} reason={e}", exc_info=True)

    # 要件：成功後のみ　mv　更新
    if ng == 0 and ok > 0:
        refresh_mv()
        logger.info("mv refreshed")
    else:
        logger.warning(f"mv skipped because ng={ng} ok={ok}")

    logger.info(f"ETL finished ok={ok} ng={ng}")
