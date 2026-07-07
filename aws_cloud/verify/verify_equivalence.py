"""Compare dbt Postgres marts vs Glue curated parquet for the same window."""
import pandas as pd, sqlalchemy, os

def load_dbt(table, engine):
    return pd.read_sql(f"select * from marts.{table}", engine)

def load_glue(path):
    return pd.read_parquet(path)  # downloaded curated/ parquet

def compare(dbt_df, glue_df, keys, metrics):
    m = dbt_df.merge(glue_df, on=keys, suffixes=("_dbt", "_glue"))
    report = {"dbt_rows": len(dbt_df), "glue_rows": len(glue_df), "matched": len(m)}
    for col in metrics:
        report[f"{col}_max_abs_diff"] = (m[f"{col}_dbt"] - m[f"{col}_glue"]).abs().max()
    return report
# document known diffs: period_date timezone, completeness rounding, sha2 vs dbt_utils key