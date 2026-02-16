from __future__ import annotations

from pathlib import Path
import pandas as pd

def load_house_map(path: str | Path) -> dict[tuple[str, str], str]:
    df = pd.read_csv(path)
    m = {}
    for _, r in df.iterrows():
        key = (str(r["month"]).strip(), str(r["company"]).strip())
        m[key] = str(r["house"]).strip()
    return m

def resolve_house(month: str, company: str, house_map: dict[typle[str, str], str]) -> str | None:
    return house_map.get((month, company))
