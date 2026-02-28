from __future__ import annotations

from typing import Any, Dict, List

from etl.harvest.schema import HarvestCsvRow

def transform_rows(rows: List[HarvestCsvRow]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for r in rows:
        company = r.company.strip()
        crop = r.crop.strip()

        out.append(
            {
                "company": company,
                "crop": crop,
                "month": r.harvest_date.strftime("%Y-%m"),
                "total_kg": float(r.amount_g) / 1000.0,
            }
        )

    return out
