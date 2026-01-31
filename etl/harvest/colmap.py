from __future__ import annotations

import os
import pandas as pd
from typing import Dict, List

# 列名マッピング
COLUMN_MAP: Dict[str, str] = {
    "収穫日": "harvest_date",
    "日付": "havest_date",
    "harvest_date": "harvest_date",

    "企業名": "company",
    "company": "company",

    "収穫野菜名": "crop",
    "crop": "crop",

    "収穫量（ｇ）": "amount_g",
    "収穫量(g)": "amount_g",
    "amount_g": "amount_g",

}

# 列名正規化
def rename_columns(columns: List[str]) -> Dict[str, str]:
    mapping = {}
    for c in columns:
        key = c.strip()
        if key in COLUMN_MAP:
            mapping[c] = COLUMN_MAP[key]
    return mapping

# 値の正規化
def normalize_values(df: ps.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 日付:YYYY/DD/MM → YYYY-MM-DD
    if "harvest_date" in df.columns:
        df["harvest_date"] = (
            df["harvest_date"]
            .astype(str)
            .str.strip()
            .str.replace("/", "-", regex=False)
        )

    # 数値：文字列　→　数値（失敗したら NaN）
    if "amount_g" in df.columns:
        df["amount_g"] = pd.to_numeric(df["amount_g"], errors="coerce")

    return df
