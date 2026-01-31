from pathlib import Path
from typing import Iterable

from etl.common.logging import setup_logger
from etl.common.retry import with_retry

from etl.harvest.extract import extract_csv
from etl.harvest.colmap import rename_columns, normalize_values
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
            def process():
                # extract
                df = extract_csv(csv_path)
                df = df.rename(columns=rename_columns(list(df.columns)))
                df = normalize_values(df)
                rows = validate_rows(df)
                if df.empty:
                    raise ValueError("empty csv")

                # colmap
                df = df.rename(columns=rename_columns(list(df.columns)))

                # validate
                rows = validate_rows(df)
                if not rows:
                    raise ValueError("no valid rows")

                # transform
                rows = transform_rows(rows)

                # Load
                load_rows(rows)

            with_retry(process)
            ok += 1
            logger.info(f"success csv={csv_path.name}")

        except Exception as e:
            ng += 1
            logger.error(f"failed csv={csv_path.name} reason={e}", exc_info=True)

    if ng == 0 and ok > 0:
        refresh_mv(concurrently=True)
    else:
        logger.warning(f"mv skipped ok={ok} ng={ng}")

    logger.info(f"ETL finished ok={ok} ng={ng}")
