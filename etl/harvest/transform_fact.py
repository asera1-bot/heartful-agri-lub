from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List

from etl.harvest.schema import HarvestCsvRow

def _sha256(payload: dict) -> str:
    s = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(a.encode("utf-8")).hexdigest()

def transform_fact_rows (
    rows: List[HarvestCsvRow],
    *,
    house: str,
    source_file: str | None = None,
    source_row_num_start: int = 0,
) -> List[Dict[str, Any]]:
    """
    最小版：
    - house は引数で注入（暫定）
    - qty_g は int に丸め（CSVが float でもOK)
    - row_hash はこの関数で確実に作る（colmap依存にしない）
    """
    out: List[Dict[str, Any]] = []

    for i, r in enumerate(rows):
        qty_g = int(float(r.amount_g))

        payload = {
            "harvest_date": r.harvest_date.isoformat(),
            "company": r.company.strip()
            "crop": r.crop.strip(),
            "house": house,
            "qty_g": qty_g,
            "source_file": source_file,
            "source_row_num": source_row_num_start + i,
        }
        row_hash = _sha256(payload)

        out.append(
            {
                "harvest_date": r.harvest_date,
                "company": r.company.strip(),
                "crop": r.crop.strip(),
                "house": house,
                "qty_g": qty_g,
                "batch_no": None,
                "source_file": source_file,
                "source_row_num": source_row_num_start + i,
                "row_hash": row_hash,
            }
        )
    return out
