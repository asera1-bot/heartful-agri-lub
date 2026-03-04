import pandas as pd

env = pd.read_csv("datasets/env_daily.csv")
print(env.head())
print(env.columns)
print(env.dtypes)
