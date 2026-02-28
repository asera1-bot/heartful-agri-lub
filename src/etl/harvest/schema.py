from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict

class RejectReason(str, Enum):
    MISSING__REQUIRED = "missing_required"
    TYPE_ERROR = "type_error"
    INVALID_VLAUE = "invalid_value"
    INVALID_DATE = "invalid_date"
    INVALID_AMOUNT = "invalid_amount"
    UNKNOWN_COMPANY = "unknown_company"
    UNKNOWN_CROP = "unknown_crop"
    OTHER = "other"

class RejectDetail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: RejectReason
    message: str
    field: Optional[str] = None
    raw: Optional[Any] = None
    source_row_num: Optional[int] = None

class HarvestRaw(BaseModel):
    # Extract直後:揺れ許容
    model_config = ConfigDict(extra="allow")

    run_id: str
    source: str = "unknown"
    source_row_num: Optional[int] = None

    harvest_date: Any = None
    company_raw: Optional[str] = None
    crop_raw: Optional[str] = None
    amount_g: Any = None

class HarvestValidated(BaseModel):
    # Data Contract :確定
    model_config = ConfigDict(extra="forbid")

    run_id: str
    source: str
    source_row_num: Optional[int] = None

    harvest_date: date
    company_key: str
    crop_key: str
    amount_g: float

    # 任意の分析軸（将来）
    ki: Optional[int] = None
    house: Optional[str] = None
    tier: Optional[str] = None
    variety: Optional[str] = None
    worker_id: Optional[str] = None

@dataclass(frozen=True)
class ValidationResult:
    ok: list[HarvestValidated]
    ng: list[tuple[HarvestRaw, RejectDetail]]
