import pandas as pd

df = pd.read_csv("data/Sample - Superstore.csv", encoding="cp1252")
df.columns = (
    df.columns.str.strip().str.lower()
    .str.replace(" ", "_").str.replace("-", "_")
)
df.to_csv("data/superstore_clean.csv", index=False, encoding="utf-8")
print(df.columns.tolist())
