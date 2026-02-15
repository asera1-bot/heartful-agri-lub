from __future__ import annotations

import os
import pandas as pd
from typing import Dict, List

import hashlib
import json

# 列名マッピング
COLUMN_MAP: Dict[str, str] = {
    "収穫日": "harvest_date",
    "日付": "harvest_date",
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
    mapping: Dict[str, str] = {}
    for c in columns:
        key = str(c).strip()
        if key in COLUMN_MAP:
            mapping[c] = COLUMN_MAP[key]
    return mapping

# 値の正規化
def normalize_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 日付:YYYY/DD/MM → YYYY-MM-DD
    if "harvest_date" in df.columns:
        s = df["harvest_date"].astype(str).str.strip()
        dt = pd.to_datetime(s, errors="coerce", yearfirst=True)
        df["harvest_date"] = dt.dt.strftime("%Y-%m-%d") # Nat is NaN

    # 数値：文字列　→　数値（失敗したら NaN）
    if "amount_g" in df.columns:
        df["amount_g"] = pd.to_numeric(df["amount_g"], errors="coerce")

    # 文字列：前後空白除去
    for col in ("company", "crop"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    return df

def _row_hash(row: dict) -> str:
    payload = {
        "harvest_date": row.get("harvest_date"),
        "company": row.get("company"),
        "crop": row.get("crop"),
        "amount_g": row.get("amount_g"),
    }
    s = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

    if all(c in df.columns for c in ("harvest_date", "company", "crop", "amount_g")):
        df["row_hash"] = df.apply(lambda r: _row_hash(r.to_dict()), axis=1)
