from __future__ import annotations

from pathlib import Path
import re
import difflib import SequenceMatcher
import math
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
plt.rcParams["font.family"] = "Noto Sans CJK JP"
plt.rcParams["axes.unicode_minus"] = False
ROOT = Path(__file__).resolve().parent
DATASETS = ROOT / "datasets"
REPORTS = ROOT / "reports"
FIG_DIR = REPORTS / "figures"

HARVEST_PATH = DATASETS / "planting_conditions.csv"
ENV_PATH = DATASETS / "env_daily.csv"

REPORTS.mkdir(parents=True, exist_ok=True)
FIG_DIR.mkdir(parents=True, exist_ok=True)

# 59期 実験棟（Z棟）の株数 （暫定、枯死数は無視）
PLANT_COUNTS_59 = {
    ("よつぼし", "Z", "上"): 34,
    ("よつぼし", "Z", "中"): 140,
    ("よつぼし", "Z", "下"): 22,
    ("やよいひめ", "Z", "上"): 72,
    ("やよいひめ", "Z", "中"): 210,
    ("やよいひめ", "Z", "下"): 20,
    ("紅ほっぺ", "Z", "上"): 72,
    ("紅ほっぺ", "Z", "中"): 210,
    ("紅ほっぺ", "Z", "下"): 144,
    ("かおり野", "Z", "上"): 32,
    ("かおり野", "Z", "中"): 140,
    ("かおり野", "Z", "下"): 34,
}

COMPANY_MAP = {
    "ｚ": "Z",
    "z": "Z",
    "Ｚ": "Z",
    "アドビ": "Adobe",
    "Naito": "NaITO",
    "Ｎａｉｔｏ": "NaITO",
}

HOUSE_MAP = {
    "ｚ": "Z",
    "z": "Z",
    "Ｚ": "Z",
}
ALLOWED_COMPANIES = {
    "IIM",
    "IDAJ",
    "×",
    "Adobe",
    "イソップ",
    "岡部",
    "QB",
    "ケイアイ",
    "サンセイランディック",
    "サンテレホン",
    "三和",
    "慈誠会",
    "昭和女子",
    "ソリトン",
    "東レ",
    "NaITO",
    "日建",
    "日本ファブテック",
    "日本ロレアル",
    "バリュエンス",
    "富士機械",
    "富士機材",
    "牧野フライス",
    "マルテー",
}

ALLOWED_VARIETIES = {
    "紅ほっぺ",
    "かおり野",
    "やよいひめ",
    "よつぼし",
}

MANUAL_FIX_MAP = {
    "処理": {
        "断": "通常",
    },
}

def normalize_text(x: object) -> object:
    if pd.isna(x):
        return x
    s = str(x).strip()
    s = s.replace("\u3000", " ")
    return s

def try_fix_mojibake_utf8_cp932(x: object) -> object:
    if pd.isna(x):
        return x
    s = str(x)
    
    try:
        fixed = s.encode("cp932").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s

    return fixed

def apply_manual_known_fixes(x: object) -> object:
    if pd.isna(x):
        return x
    s = str(x)
    fix_map = {
        "繝槭Ν繝・・": "マルテー",
        "邏・⊇縺｣縺ｺ": "よつぼし",
        "断": "通常",
    }
    return fix_map.get(s, s)

def repair_mojibake_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].map(try_fix_mojibake_utf8_cp932)
    return out

def apply_manual_fix_map(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col, fix_map in MANUAL_FIX_MAP.items():
        if col in out.columns:
            out[col] = out[col].replace(fix_map)
    return out

def validate_master_values(df: pd.DataFrame) -> None:
    if "企業名" in df.columns:
        bad_companies = sorted(set(df["企業名"].dropna()) - ALLOWED_COMPANIES)
        if bad_companies:
            print("[unknown companies]", bad_companies)

    if "品種_base" in df.columns:
        bad_varieties = sorted(set(df["品種_base"].dropna()) - ALLOWED_VARIETIES)
        if bad_varieties:
            print("[unknown varieties]", bad_varieties)

def longest_common_substring_len(a: str, b: str) -> int:
    if not a or not b:
        return 0
    dp = [[0] * (len(b) + 1) for _ in range(len(a) + 1)]
    best = 0
    for i in range(1, len(a) + 1):
        for j in range(1, len(b) + 1):
            if a[i - 1] == b[j - 1]:
                dp[i][j] = dp[i - 1][j - 1] + 1
                best = max(best, dp[i][j])
    return best

def similarity_score(src: str, cand: str) -> float:
    src = str(src)
    cand = str(cand)

    ratio = SequenceMatcher(None, src, cand).ratio()
    common_chars = len(set(src) & set(cand))
    common_ratio = common_chars / max(len(set(cand)), 1)
    lcs_ratio = longest_common_substring_len(src, cand) / max(len(cand), 1)

    return ratio * 0.5 + common_ratio * 0.2 + lcs_ratio * 0.3

def suggest_best_match(value: object, candidates: list[str], threshold: float = 0.55) -> object:
    if pd.isna(value):
        return value

    s = str(value).strip()
    if not s:
        return s

    if s in candidates:
        return s

    scored = [(cand, similarity_score(s, cand)) for cand in candidates]
    scored.sort(key=lambda x: x[1], reverse=True)

    best_cand, best_score = score[0]
    return best_cand if best_score >= threshold else s

def apply_fuzzy_master_matching(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "企業名" in out.columns:
        out["企業名"] = out["企業名"].map(lambda x: suggest_best_match(x, ALLOWED_COMPANIES, threshold=0.50))
        #続きから
def clean_harvest(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [normalize_text(c) for c in out.columns]

    required = ["日付", "期", "企業名", "ハウスNo", "段", "品種", "処理", "収量", "パック"]
    missing = [c for c in required if c not in out.columns]
    if missing:
        raise ValueError(f"planting_conditions.csv に必要列がありません: {missing}")

    out["日付"] = pd.to_datetime(out["日付"], errors="coerce")
    out["期"] = pd.to_numeric(out["期"], errors="coerce")
    for c in ["企業名", "ハウスNo", "段", "品種", "処理"]:
        out[c] = out[c].map(normalize_text)
        out[c] = out[c].map(try_fix_mojibake_utf8_cp932)
        out[c] = out[c].map(apply_manual_known_fixes)

    out = repair_mojibake_columns(
        out,
        ["企業名", "ハウスNo", "段", "品種", "処理"]
    )

    out["企業名"] = out["企業名"].replace(COMPANY_MAP)
    out["ハウスNo"] = out["ハウスNo"].replace(HOUSE_MAP)
    out["段"] = out["段"].replace({"上段": "上", "中段": "中", "下段": "下"})

    out["収量"] = pd.to_numeric(out["収量"], errors="coerce")
    out["パック"] = pd.to_numeric(out["パック"], errors="coerce")

    # 品種のベース名（処理は別名で保持）
    out["品種_base"] = out["品種"].str.replace(r"（.*?）", "", regex=True)
    out["品種_base"] = out["品種_base"].str.replace(r"\(.*?\)", "", regex=True)
    out["品種_base"] = out["品種_base"].map(normalize_text)

    out["処理"] = out["処理"].fillna("通常")
    out["パック推定"] = np.where(out["収量"].notna(), out["収量"] / 100.0, np.nan)
    out["パック最終"] = out["パック"].fillna(out["パック推定"])

    # 基本分析対象>0, 段あり, 日付あり　を使う
    out = out[out["日付"].notna()].copy()
    out = out[out["収量"].fillna(0) > 0].copy()
    out = out[out["段"].notna()].copy()

    out["date"] = out["日付"].dt.normalize()
    return out

def load_env(path: Path) -> pd.DataFrame:
    env = pd.read_csv(path)
    env["date"] = pd.to_datetime(env["date"], errors="coerce").dt.normalize()
    env = env.dropna(subset=["date"]).copy()
    return env

def attach_env_same_day(harvest: pd.DataFrame, env: pd.DataFrame) -> pd.DataFrame:
    h = harvest.copy()
    e = env.copy()
    h["date"] = pd.to_datetime(h["date"], errors="coerce")
    joined = h.merge(e, on="date", how="left")
    env_cols = [c for c in e.columns if c != "date"]
    joined["env_hit"] = joined[env_cols].notna().any(axis=1)
    return joined

def add_plant_counts(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    def lookup(row: pd.Series) -> float:
        key = (row["品種_base"], row["ハウスNo"], row["段"])
        if pd.notna(row["期"]) and int(row["期"]) == 59 and key in PLANT_COUNTS_59:
            return PLANT_COUNTS_59[key]
        return np.nan

    out["株数"] = out.apply(lookup, axis=1)
    out["株当たり収量"] = out["収量"] / out["株数"]
    return out

def summary_by_variety_level(df: pd.DataFrame) -> pd.DataFrame:
    work = add_plant_counts(df).copy()
    return (
        work.groupby(["期", "ハウスNo", "段", "品種_base"], dropna=False)
        .agg(
            n_records=("日付", "count"),
            total_yield_g=("収量", "sum"),
            total_packs=("パック最終", "sum"),
            mean_yield_g=("収量", "mean"),
            plants=("株数", "max"),
            yield_per_plant_g=("株当たり収量", "sum"),
        )
        .reset_index()
        .sort_values(["期", "品種_base", "段"])
    )

def summary_env_hit(df: pd.DataFrame) -> pd.DataFrame:
    work = add_plant_counts(df).copy()
    env_cols = [
        c for c in work.columns
        if c.startswith(("temp_c_", "rh_pct_", "vpd_kpa_", "sand_temp_c_", "lux_", "vwc_"))
    ]
    hit = work[work[env_cols].notna().any(axis=1)].copy() if env_cols else work.iloc[0:0].copy()
    if hit.empty:
        return pd.DataFrame()

    cols = {
        "temp_c_mean": "temp_mean",
        "rh_pct_mean": "rh_mean",
        "vpd_kpa_mean": "vpd_mean",
        "sand_temp_c_mean": "sand_temp_mean",
        "lux_mean": "lux_mean",
    }

    agg_map = {
        "日付": ("日付", "count"),
        "収量": ("収量", "sum"),
        "パック最終": ("パック最終", "sum"),
        "株当たり収量": ("株当たり収量", "sum"),
    }

    for src in cols:
        if src in hit.columns:
            agg_map[src] = (src, "mean")

    out = (
        hit.groupby(["期", "ハウスNo", "段", "品種_base"], dropna=False)
        .agg(**{k: v for k, v in agg_map.items()})
        .reset_index()
        .rename(columns={
            "日付": "n_env_records",
            "収量": "env_hit_total_yield_g",
            "パック最終": "env_hit_total_packs",
            "株当たり収量": "env_hit_yield_per_plant_g",
            **cols,
        })
    )
    return out.sort_values(["期", "品種_base", "段"])

def cumulative_table(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    daily = (
        work.groupby(["期", "ハウスNo", "段", "品種_base", "日付"], dropna=False)["収量"]
        .sum()
        .reset_index()
        .sort_values(["期", "ハウスNo", "段", "品種_base", "日付"])
    )
    daily["累積収量_g"] = daily.groupby(["期", "ハウスNo", "段", "品種_base"])["収量"].cumsum()
    return daily

def company_house_consistency(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    grp = (
        work.groupby(["期", "企業名", "品種_base"], dropna=False)["ハウスNo"]
        .agg(lambda s: sorted(set(x for x in s if pd.notna(x))))
        .reset_index(name="houses")
    )
    grp["house_count"] = grp["houses"].apply(len)
    return grp.sort_values(
        ["house_count", "期", "企業名", "品種_base"],
        ascending=[False, True, True, True]
    )

def save_excel(outputs: dict[str, pd.DataFrame], path: Path) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet, df in outputs.items():
            df.to_excel(writer, sheet_name=sheet[:31], index=False)

def setup_matplotlib_font() -> None:
    candidates = [
        "Noto Sans CJK JP",
        "Noto Sans JP",
        "IPAexGothic",
        "IPAGothic",
        "Yu Gothic",
        "MS Gothic",
    ]
    available = {f.name for f in fm.fontManager.ttflist}
    selected = next((name for name in candidates if name in available), None)

    if selected:
        plt.rcParams["font.family"] = selected

    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["figure.autolayout"] = True

def plot_yield_per_plant_59ki(df: pd.DataFrame, out_path: Path) -> None:
    d = add_plant_counts(df).copy()
    if d.empty:
        return

    if "期" in d.columns:
        d = d[d["期"].astype("Int64") == 59].copy()
    if d.empty:
        return

    d["品種_base"] = d["品種_base"].astype(str)
    d["段"] = d["段"].astype(str)

    value_col = "株当たり収量"

    agg = (
        d.groupby(["品種_base", "段"], dropna=False)[value_col]
        .sum()
        .reset_index()
        .rename(columns={"品種_base": "品種", value_col: "yield_per_plant_g"})
    )

    stage_order = ["上", "中", "下"]
    agg["段"] = pd.Categorical(agg["段"], categories=stage_order, ordered=True)
    agg = agg.sort_values(["品種", "段"])

    pivot = agg.pivot(index="品種", columns="段", values="yield_per_plant_g")
    pivot = pivot.reindex(columns=stage_order)

    ax = pivot.plot(kind="bar", figsize=(12, 6), rot=0)
    ax.set_title("59期 株当たり収量比較 （品種×段）")
    ax.set_xlabel("品種")
    ax.set_ylabel("株当たり収量（g/株）")
    ax.legend(title="段")
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    plt.tight_layout()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()

def plot_cumulative_by_variety(df: pd.DataFrame, variety: str, out_path: Path) -> None:
    d = df.copy()

    if "期" in d.columns:
        d = d[d["期"].astype(str) == "59"].copy()

    d = d[d["品種"].astype(str) == str(variety)].copy()
    if d.empty:
        return

    date_col = "date" if "date" in d.columns else "日付"
    yield_col = "収量" if "収量" in d.columns else "yield_g"

    d[date_col] = pd.to_datetime(d[date_col], errors="coerce")
    d = d.dropna(subset=[date_col])
    d["段"] = d["段"].astype(str)

    daily = (
        d.groupby([date_col,"段"], dropna=False)[yield_col]
        .sum()
        .reset_index()
        .sort_values([date_col, "段"])
    )

    stage_order = ["上", "中", "下"]

    plt.figure(figsize=(12, 6))
    for stage in stage_order:
        s = daily[daily["段"] == stage].copy()
        if s.empty:
            continue
        s = s.sort_values(date_col)
        s["累積収量_g"] = s[yield_col].cumsum()
        plt.plot(s[date_col], s["累積収量_g"], marker="o", label=stage)

    plt.title(f"59期 累積収量推移({variety})")
    plt.xlabel("日付")
    plt.ylabel("累積収量（g）")
    plt.legend(title="段")
    plt.grid(True, linestyle="--", alpha=0.4)

    plt.tight_layout()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()

def plot_box_yield_distribution(df: pd.DataFrame, out_path: Path) -> None:
    d = df.copy()

    if "期" in d.columns:
        d = d[d["期"].astype(str) == "59"].copy()

    if "env_hit" in d.columns:
        d = d[d["env_hit"].fillna(False)]

    if d.empty:
        return

    yield_col = "収量" if "収量" in d.columns else "yield_g"

    d["品種"] = d["品種"].astype(str)
    d["段"] = d["段"].astype(str)
    d["品種_段"] = d["品種"] + "\n" + d["段"]

    stage_rank = {"上": 0, "中": 1, "下": 2}
    order_df = (
        d[["品種", "段", "品種_段"]]
        .drop_duplicates()
        .assign(_rank=lambda x: x["段"].map(stage_rank).fillna(99))
        .sort_values(["品種", "_rank"])
    )
    order = order_df["品種_段"].tolist()

    data = [
        d.loc[d["品種_段"] == key, yield_col].dropna().values
        for key in order
    ]
    data = [x for x in data if len(x) > 0]
    labels = [key for key in order if len(d.loc[d["品種_段"] == key, yield_col].dropna()) > 0]

    plt.figure(figsize=(14, 6))
    plt.boxplot(data, tick_labels=labels, patch_artist=False)
    plt.title("59期 収量分布 （品種×段, env_hitのみ）")
    plt.xlabel("品種 × 段")
    plt.ylabel("収量（g）")
    plt.xticks(rotation=0)
    plt.grid(axis="y", linestyle="--", alpha=0.4)

    plt.tight_layout()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()

def generate_presentation_figures(df: pd.DataFrame, figures_dir: Path) -> None:
    figures_dir.mkdir(parents=True, exist_ok=True)
    setup_matplotlib_font()

    plot_yield_per_plant_59ki(
        df,
        figures_dir / "01_bar_yield_per_plant_59ki.png",
    )

    for variety in ["よつぼし", "紅ほっぺ", "かおり野", "やよいひめ"]:
        plot_cumulative_by_variety(
            df,
            variety,
            figures_dir / f"02_cumulative_yield_59ki_{variety}.png",
        )

    plot_box_yield_distribution(
        df,
        figures_dir / f"03_box_yield_distribution_59ki_envhit.png",
    )

def plot_vpd_scatter(joined: pd.DataFrame) -> None:
    import matplotlib.pyplot as plt

    df = joined.copy()
    if df.empty:
        return

    target = df[(df["期"] == 59) & (df["env_hit"])].copy()
    if target.empty or "vpd_kpa_mean" not in target.columns:
        return

    fig, ax = plt.subplots(figsize=(13, 7))
    for variety, sub in target.groupby("品種_base"):
        ax.scatter(sub["vpd_kpa_mean"], sub["収量"], label=variety)

    valid = target[["vpd_kpa_mean", "収量"]].dropna()
    if len(valid) >= 2:
        z = np.polyfit(valid["vpd_kpa_mean"], valid["収量"], 1)
        xs = np.linspace(valid["vpd_kpa_mean"].min(), valid["vpd_kpa_mean"].max(), 100)
        ys = z[0] * xs + z[1]
        ax.plot(xs, ys, linewidth=1)

    ax.set_title("59期 実験棟 VPD平均 × 収量")
    ax.set_xlabel("VPD平均(kpa)")
    ax.set_ylabel("収量(g)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(FIG_DIR / "04_scatter_vpd_vs_yield_59ki.png", dpi=200)
    plt.close(fig)

def main() -> None:
    if not HARVEST_PATH.exists():
        raise FileNotFoundError(f"not found: {HARVEST_PATH}")
    if not ENV_PATH.exists():
        raise FileNotFoundError(f"not found: {ENV_PATH}")

    raw = pd.read_csv(HARVEST_PATH)
    harvest = clean_harvest(raw)
    print(harvest[["企業名", "ハウスNo", "段", "品種", "処理"]].head(10))
    env = load_env(ENV_PATH)
    joined = attach_env_same_day(harvest, env)

    summary_all = summary_by_variety_level(harvest)
    summary_joined = summary_env_hit(joined)
    cumulative = cumulative_table(harvest)
    consistency = company_house_consistency(harvest)

    cleaned_path = REPORTS / "harvest_master_cleaned.csv"
    joined_path = REPORTS / "harvest_env_joined_same_day.csv"
    xlsx_path = REPORTS / "analytics_summary.xlsx"

    harvest.to_csv(cleaned_path, index=False, encoding="utf-8-sig")
    joined.to_csv(joined_path, index=False, encoding="utf-8-sig")

    save_excel(
        {
            "harvest_master_cleaned": harvest,
            "summary_all": summary_all,
            "summary_env_hit": summary_joined,
            "cumulative": cumulative,
            "consistency_chech": consistency,
        },
        xlsx_path,
    )

    generate_presentation_figures(joined, FIG_DIR)
    hit_rate = joined["env_hit"].mean() if ("env_hit" in joined.columns and len(joined)) else float("nan")

    print(f"saved: {cleaned_path}")
    print(f"saved: {joined_path}")
    print(f"saved: {xlsx_path}")
    print(f"saved figures: {FIG_DIR}")
    print(f"rows_harvest={len(harvest)} rows_joined={len(joined)} ecn_hit_rate={hit_rate:.3f}")

if __name__ == "__main__":
    main()
