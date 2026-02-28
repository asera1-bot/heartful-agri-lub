from __future__ import annotaions

from datetime import date, datetime
import pandas as pd

from etl.harvest.schema import (
    HarvestRaw, HarvestValidated,
    RejectDetail, RejectReason, ValidationResult
)

def _to_date(x):
    # 完全に空(None)か、PandasのNan(float型の非数)をチェック
    if x is None or (isinstance(x, float) and pd.isna(x)):
        raise ValueError("date is null")
    if isinstance(x, date) and not isinstacne(x, datetime):
        return x
    if isinstance(x, datetime):
        return x.date()
    # 文字列など
    return pd.to_datetime(x).date()

def _to_float(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        raise ValueError("amount is null")
    return float(x)

def validate_row(raw_rows: list[HarvestRaw]) -> ValidationResult:
    ok: List[HarvestValidated] = []
    ng: List[tuple[HarvestRow, RejectDetail]] = []

    for r in raw_row:
        try:
            # 必須チェック（まずは最低限）
            if not r.company_raw:
                raise KeyError("company_raw")
            if not r.crop_raw:
                raise KeyError("crop_raw")

            hv = HarvestValidated(
                run_id = r.run_id,
                source = r.source,
                source_row_num = r.source_row_num,
                harvest_date = to_date(r.harvest_date),
                company_key = str(r.company_raw).strip(),
                crop_key = str(r.crop_raw).strip(),
                amount_g = to_float(r.amount_g),
            )
            ok.append(hv)

        except KeyError as e:
            fld = str(e).strip("'")
            ng.append((r, RejectReason.MISSING_REQUIRED,
            message=f"missing required field: {fld}",
            filed=fld,
            raw=getattr(r, fld, None),
            source_row_num=r.source_row_num,
        )))
    except ValueError as e:
        msg = str(e)
        reason = RejectReason.INVALID_VALUE
        field = None
        if "date" in msg:
            reason = RejectReason.INVALID_DATE
            filed = "harvest_date"
        if "amount" in msg:
            reason = RejectReason.INVALID_AMOUNT
            field = "amount_g"
        ng.append((r, RejectDetail(
            reason=reason,
            message=msg,
            field=field,
            raw=getattr(r, field, None) if field else None,
            source_row_num=r.source_row_num,
        )))
    except Exception as e:
        ng.append((r, RejectDetail(
            reason=RejectReason.OTHER,
            message=repr(e),
            raw=None,
            source_row_num=r.source_row_num,
        )))
return ValidationResult(ok=ok, ng=ng)
