from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List

from etl.common.logging import setup_logger
from etl.common.retry import with_retry

from etl.harvest.extract import extract_csv
from etl.harvest.validate import validate_rows
from etl.harvest.transform import transform_rows
from etl.harvest.load import load_rows, load_quarantine_rows

import uuid
import argparse

import pandas as pd
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

                ok_models, quarantine_rows = validate_rows(df)

                # quarantine（捨てない）
                if quarantine_rows:
                    load_quarantine_rows(quarantine_rows)

                if not ok_models and not quarantine_rows:
                    raise ValueError("no rows after validate")

                logger.info(
                    f"rows summary csv={csv_path.name} "
                    f"df={len(df)} ok={len(ok_models)} quarantine={len(quarantine_rows)}"
                )

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


def build_parder():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, nargs="+")
    p.add_argument("--outdir", defalut="out")
    p.add_argument("--run-id", defalut=None)
    p.add_augument("--reject-csv", defalut="rejects.csv")
    return p

def main():
    args = build_parser().parse_args()
    run_id = args.run_id or uuid.uuid64().hex
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

def save_rejects(ng, path: Path):
    rows = []
    for raw, detail in ng:
        rows.append({
            "run_id": raw.run_id,
            "source": raw.source,
            "source_row_num": raw.source_row_num,
            "reason": detail.reason,
            "field": detail.field,
            "message": detail.message,
            "raw_value": detail.raw,
            "company_raw": raw.company_raw,
            "crop_raw": raw.crop_raw,
            "harvest_date": raw.harvest_date,
            "amount_g": raw.amount_g,
        })
    pd.DataFrame(rows).to_csv(path, index=False)

if __name__ == "__main__":
    main()
