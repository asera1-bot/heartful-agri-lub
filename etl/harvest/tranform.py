from __future__ import annotations
from typing import List, Dict, Any

from etl.harvest.rename import HarvestCsvRow

def transform(rows: list[HarvestCsvRow]) -> list[dict]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "company": r.company.strip(),
            "crop": r.crop.strip(),
            "month": r.harvest_date.strftime("%Y-%m"),
            "total_kg": r.amount_g / 1000.0,
        })
    return out
