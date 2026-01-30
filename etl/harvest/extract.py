from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from tel.common.logging import setup_logger

logger = setup_logger("harvest_etl")

DEFAULT_ENCODINGS: tuple[str,...] = (
    "cp932",        #Windows日本語（最有力）
    "shift_jis",    #念のため
    "utf-8-sig",    #BOM付きUTF-8
    "utf-8",        #UTF-8
)

def _read_csv_with_fallback(
    path: Path,
    encodings: Iterable[str] = DEFAULT_ENCODINGS,
    **read_csv_kwargs,
) -> pd.Dataframe:
    last_err: Optional[Exception] = None
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc, **read_csv_kwargs)
            logger.info(f"read_csv_file={path.name} encoding={enc} rows={len(df)} cols={len(df.columns)}")
            return df
        except Exception as e:
            last_err = e
            logger.warning(f"read_csv failed file={path.name} encoding={enc} reason={e}")
    raise RuntimeError(f"read_csv failed for all encodings file={path.name}") from last_err

def extract_csv(path: str | Path) -> pd.DataFrame:
    """
    extractの責務:
    - ファイル単位の異常を検知して skip / 例外
    - 文字コードを正しく読む
    - DataFrameを返す（中身は基本いじらない）

    ここでは「列名の正規化(colmap)」や「型変換」はやらない。
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(str(path))

    if path.stat().st_size == 0:
        #　空ファイルはCSV単位でスキップされるべき
        logger.warning(f"empty file skipped file={path.name}")

    # 重要:dtypeは固定しない（文字列混在があると事故る）
    # na_filter=False　で文字列を　NaN　にしない（現場はCSVは空欄多い想定）
    df = _read_csv_with_fallback(
        path,
        na_filter=False,
    )

    #　0行はスキップ
    if df.empty:
        logger.warning(f"no rows skipped file={path.name}")
        return df

    # 余計な全角スペースや前後空白だけ軽く掃除（値変換ではない）
    df.columns = [str(c).strip().replace("　"," ") for c in df.columns]

    return df
