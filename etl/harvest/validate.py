from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import pandas as pd
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

def validate_rows(df: pd.DataFrame) -> tuple[list[HarvestCsvRow], list[QuarantineRow]]:
    ok: list[HarvestCsvRow] = []
    ng: list[QuarantineRow] = []

    records = df.to_dict(orient="records")
    for i, rec in enumerate(records):
        if not isinstance(rec, dict):
            raise TypeError(f"records is not dict idx={i} type={type(rec)} value={rec!r}")
        try:
            # pydanticモデルでバリデーション
            ok.append(HarvestCsvRow(**rec))
        except ValidationError as e:
            ng.append(
                QuarantineRow(
                    idx=i,
                    reason="contract_violation",
                    details={"errors": e.errors()},
                    raw=rec,
                )
            )
    logger.info(f"validate done ok={len(ok)} quarantine={len(ng)}")
    return ok, ng
