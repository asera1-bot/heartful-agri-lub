import pandas as pd

df = pd.read_csv("reports/harvest_master_cleaned.csv", encoding="utf-8-sig")
print(df.head())
print(df.columns.tolist())
