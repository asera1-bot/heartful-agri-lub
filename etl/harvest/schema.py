from pydantic import BaseModel, Field
from datetime import date

class HarvestCsvRow(BaseModel):
    harvest_date: date
    company: str = Field(min_length=1)
    crop: str = Field(min_length=1)
    amount_g: float = Field(gt=0)
