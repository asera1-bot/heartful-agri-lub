# etl/harvest/run.py
from __future__ import annotations

from pathlib import Path
from typing import Iterable

from etl.common.logging import setup_logger
from etl.common.retry import with_retry

from etl.harvest.extract import extract_csv
from etl.harvest.colmap import rename_columns, normalize_values
from etl.harvest.validate import validate_rows
from etl.harvest.transform import transform_rows
from etl.harvest.load import load_rows, load_quarantine_rows, load_fact_rows
from etl.harvest.house_resolve import load_house_map
from etl.harvest.transform_fact import transform_fact_rows, split_fact_and_house_quarantine
logger = setup_logger("harvest_etl")


def run(csv_files: Iterable[Path]) -> None:
    ok_files = 0
    ng_files = 0

    for csv_path in csv_files:
        logger.info(f"start csv={csv_path.name}")

        try:
            def process():
                df = extract_csv(csv_path)
                if df.empty:
                    raise ValueError("empty csv")

                df = df.rename(columns=rename_columns(list(df.columns)))
                df = normalize_values(df)

                ok_models, quarantine_rows = validate_rows(df)
                load_quarantine_rows(quarantine_rows)

                if not ok_models and not quarantine_rows:
                    raise ValueError("no rows after validate")

                logger.info(
                    f"rows summary csv={csv_path.name} "
                    f"df={len(df)} ok={len(ok_models)} quarantine={len(quarantine_rows)}"
                )

                # house_map (config) 元fact
                house_map = load_house_map("/app/etl/harvest/house_map.csv")

                fact_rows, house_q_rows = split_fact_and_house_quarantine(
                    ok_models,
                    house_map=house_map,
                    source_file=csv_path.name,
                    source_row_num_start=0,
                )

                # house未解決も捨てずに隔離
                load_quarantine_dict_rows(house_q_rows)

                # fact_only
                load_fact_rows(fact_rows)

                # legacy monthly（当面維持）
                monthly_rows = transform_rows(ok_models)
                load_rows(monthly_rows)

            with_retry(process)
            ok_files += 1
            logger.info(f"success csv={csv_path.name}")

        except Exception as e:
            ng_files += 1
            logger.error(f"failed csv={csv_path.name} reason={e}", exc_info=True)

    logger.info(f"ETL finished ok={ok_files} ng={ng_files}")
