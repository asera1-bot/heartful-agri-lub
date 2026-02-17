from __future__ import annotations

from pathlib import Path
from typing import Iterable

from etl.common.logging import setup_logger
from etl.common.retry import with_retry

from etl.harvest.extract import extract_csv
from etl.harvest.colmap import rename_columns, normalize_values
from etl.harvest.validate import validate_rows
from etl.harvest.transform import transform_rows
from etl.harvest.transform_fact import transform_fact_rows
from etl.harvest.load import load_rows, load_quarantine_rows, load_fact_rows
#from etl.harvest.duplicate import detect_duplicates 追加
#from etl.harvest.load_fact import load_fact_rows 追加
#from etl.harvest.load_quarantine import load_quarantine_rows 追加

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

# fact:harvest_fact
def load_fact_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        logger.info("fact load skipped: rows is empty")
        return

    engine = _get_engine()
    fact = _table(engine, "harvest_fact")

    stmt = insert(fact).values(rows)

    with engine.begin() as conn:
        conn.execute(stmt)

    logger.info(f"inserted fact rows={len(rows)}")

# quarantine: harvest_quarantine
def load_quarantine_rows(quarantine_rows, *, source_file: str | None = None) -> None:
    if not quarantine_rows:
        logger.info("quarantine load skipped: rows is empty")
        return

    engine = _get_engine()
    hq = _table(engine, "harvest_quarantine")

    rows = []
    for q in quarantine_rows:
        raw = getattr(q, "raw", None) or {}
        if isinstance(raw, dict) and "detail" in raw and "details" not in raw:
            raw["details"] = raw.pop("detail")

        details = getattr(q, "details", None)
        if detail is None and hasattr(q, "detail"):
            details = getattr(q, "detail")

        row = {
            "harvest_date": raw.get("harvestDate"),
            "company": raw.get("company"),
            "house": rwaw.get("house"),
            "qty_g": raw.get("qty_g") if "qty_g" in raw else raw.get("amount_g"),
            "reason": getattr(q, "reason", "unknown"),
            "details": details,
            "raw_payload": raw,
            "row_hash": raw.get("row_hash"),
            "source_file": source_file,
            "source_row_num": getattr(q, "idx", None),
        }

        allowed = {c.name for c in hq.columns}
        extra = set(row.keys()) - allowed
        if extra:
            raise RuntimeError(f"quarantine insert has extra keys: {sorted(extra)}")

        rows.append(row)

    stmt = insert(hq).values(rows)
    with engine.begin() as conn:
        conn.execute(stmt)

    logger.info(f"inserted quarantine rows={len(rows)}")

def run(csv_files: Iterable[Path]) -> None:
    ok_files = 0
    ng_files = 0

    for csv_path in csv_files:
        logger.info(f"start csv={csv_path.name}")

        try:
            def process():
                # 1) extract
                df = extract_csv(csv_path)
                if df.empty:
                    raise ValueError("empty csv")

                # 2) colmap　1度だけ
                df = df.rename(columns=rename_columns(list(df.columns)))
                df = normalize_values(df)

                # 3) validate (ok + quarantine)
                ok_rows, quarantine_rows = validate_rows(df) # validate.pyをtuple返却に変更する前提
                load_quarantine_rows(quarantine_rows, source_file=csv_path.name)

                if not ok_rows and not quarantine_rows:
                    raise ValueError("no rows after validate")

                logger.info(f"rows summary csv={csv_path.name} df={len(df)}"
                            f"ok={len(ok_rows)} quarantine={len(quarantine_rows)}"
                )

                # 5) fact load （暫定house注入）
                fact_rows = transform_fact_rows(
                    ok_rows,
                    house="A棟",
                    source_file=csv_path.name,
                    source_row_num_start=0,
                )
                load_fact_rows(fact_rows)

                # 5) legacy monthly load 動作確認のために当面残す
                monthly_rows = transform_rows(ok_rows)
                logger.info(f"monthly_rows type={type(monthly_rows)} len={len(monthly_rows)} sample={monthly_rows[:2]}")
                load_rows(monthly_rows)

            with_retry(process)
            ok_files += 1
            logger.info(f"success csv={csv_path.name}")

        except Exception as e:
            ng_files += 1
            logger.error(f"failed csv={csv_path.name} reason={e}", exc_info=True)

    logger.info(f"ETL finished ok={ok_files} ng={ng_files}")

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

