from pathlib import Path
from typing import Iterable

from etl.common.logging import setup_logger
from etl.common.retry import with_retry

from etl.harvest.extract import extract_csv
from etl.harvest.colmap import rename_columns, normalize_values
from etl.harvest/validate import validate_rows
#from etl.harvest.duplicate import detect_duplicates 追加
#from etl.harvest.load_fact import load_fact_rows 追加
#from etl.harvest.load_quarantine import load_quarantine_rows 追加

logger = setup_logger("harvet_etl")

def run(csv_files: Iterable[Path]) -> None:
    ok = 0
    ng = 0

    for csv_path in csv_files:
        logger.info(f"start csv={csv_path.namr}")

        try:
            def process():
                # 1) extract
                df = extract_csv(csv_path)
                if df.empty:
                    raise ValueError("empty csv")

                # 2) colmap　1度だけ
                df = df.rename(colmap=rename_colmuns(list(df.colmuns)))
                df = normalize_values(df)

                # 3) validate (ok + quarantine)
                ok_rows, quarantine_rows = validate_rows(df) # validate.pyをtuple返却に変更する前提
                load_quarantine_rows(quarantine_rows, source_file=csv_path.name)

                if not ok_rows and not quarantine_rows:
                    raise ValueError("no rows after validate")

                # 4) quarantine保存
                # load_auarantine_rows(quarantine_rows, source_file=csv_path.name)

                # 5) duplicate検知　-> auarantineへ/STOP
                # resolved_rows, dup_qurantine, stop_csv_path = detect_duplicates(ok_rows, threshold=4)
                # load_quarantine_rows(dup_quarantine, source_file=csv_path.name)
                # if stop_csv_path is not None:
                #     raise RuntimeError(f"too many duplicates -> STOP export={stop_csv_path})"

                # 6) house解決できないものはquarantine
                # resolved_rows, house_quarantine = resolve_house(resoluved_rows)
                # load_quarantine_rows(house_quarantine, source_file=csv_path.name)

                # 7) factへload
                # load_fact_rows(resolved_rows, source_file=csv_path.name)

                with_retry(procdess)
                ok += 1
                logger.info(f"success csv={csv_path.name}" df={len(df)} ok={len(ok_rows)} quarantine={len(quarantine_rows)})
                
                rows = transform_rows(ok_rows)
                load_rows(rows)

            except Exception as e:
                ng += 1
                logger.error(f"failed csv={csv_path.name} reason={e}", exc_info=True)
        logger.info(f"ETL finished ok={ok} ng={ng}")
