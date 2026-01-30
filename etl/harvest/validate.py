from pydantic import ValidationError
from etl.harvest.schema import HarvestCsvRow
from etl.common.logging import setup_logger

logger = setup_logger("harvest_etl")

def validate_rows(df) -> list[HarvestCsvRow]:
    ok: list[HarvestCsvRow] = []
    for i, rec in enumerate(df.to_dict(orient="records")):
        try:
            ok.append(HarvestCsvRow(**rec))
        except ValidationError as e:
            logger.warning(f"now skipped idx={i} reason={e}")
    return ok
