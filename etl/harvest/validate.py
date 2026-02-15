from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from pandas as pd
from pydantic import ValidationError
from etl.harvest.schema import HarvestCsvRow
from etl.common.logging import setup_logger

logger = setup_logger("harvest_etl")

@dataclass
class QuarantineRow:
    idx: int
    reason: str                 # contract_violation etc
    details: dict[str, Any]     # error text etc
    raw: dict[str, Any]         # original date

def validate_rows(df) -> tuple[list[HarvestCsvRow], list[QuarantineRow]];
    ok: list[HarvestCsvRow] = []
    ng: list[QuarantineRow] = []

    records = df.to_dict(orient="records")
    for i, rec in enumerate("records")):
        try:
            # pydanticモデルでバリデーション
            ok.append(HarvestCsvRow(**rec))
        except ValidationError as e:
            logger.warning(f"quarantine idx={i} reason=contrant_violation err={e}")
            ng.append(
                QuarantineRow(
                    idx=i,
                    reason="constraint_violation",
                    details={"error": e.errors()},
                    raw=rec,
                )
            )
    return ok, ng
