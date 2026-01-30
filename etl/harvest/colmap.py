from __furure__ import annotations

import re
from typing import Dict, Mapping

def _normalize_key(s: str) -> str:
    s = str(s).strip().lower().replace("　", " ")
    s = re.sub(r"\s+", " ", s)
    s = s.replace("-", "_").replace(" ", "_")
    return s

# harvest の「正規列名」（validate　が期待するキー）
CANONICAL: List[str] = ["harvest_date", "company", "crop", "amount_g"]

# 別名　→　正規名（入力CSVの方言辞書）
_RAM_MAP: Dict[str, str] = {
    # harvest_date
    "harvest_date": "harvest_date",
    "date": "harvest_date",
    "収穫日": "harvest_date",
    "日付": "harvest_date",

    # company
    "company": "company",
    "企業": "company",
    "企業名": "company",
    "会社": "company",
    "会社名": "harvest_date",

    # crop
    "crop": "crop",
    "作物": "crop",
    "品目": "crop",
    "野菜収穫名": "crop",

    # amount_g
    "amount_g": "amount_g",
    "amount": "amount_g",
    "収穫量": "amount_g",
    "収穫量(g)": "amount_g",
    "収穫量（g）": "amount_g",
    "収穫量（ｇ）": "amount_g",
    "重量": "amount_g",
    "g": "amount_g",
	
	#　文字化け保険（encoding修正が本命）
    "蜿守ｩｫ譌･": "harvest_date",
    "莨∵･ｭ蜷・": "company",
    "蜿守ｩｫ驥手除蜷・": "crop",
    "蜿守ｩｫ驥擾ｼ茨ｽ・ｼ・": "amount_g",
}

_COLMAP: Dict[str, str] = {_normalize_key(k): v for k, v in _RAW_MAP.items()}

def rename_columns(columns: List[str]) -> Dict[str, str]:
	"""
	df.rename(columns=rename_columns(list(df.columns)))　用
	"""
	out: Dict[str, str] = {}
	for c in columns:
		key = _normalize_key(c)
		if key in _COLMAP:
			out[c] = _COLMAP[key]
	return out
